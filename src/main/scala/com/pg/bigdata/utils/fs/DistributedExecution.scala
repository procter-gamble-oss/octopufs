package com.pg.bigdata.utils.fs

import com.pg.bigdata.utils.fs
import com.pg.bigdata.utils.helpers.ConfigSerDeser
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

//val magicPrefix = ".dfs.core.windows.net"

object DistributedExecution extends Serializable {


  def copyFolder(sourceFolderUri: String, targetLocationUri: String, partitionCount: Int = 192)(implicit spark: SparkSession, confEx: Configuration): Array[FSOperationResult] = {
    val srcFs = getFileSystem(confEx, sourceFolderUri)
    val sourceFileList = listRecursively(srcFs, new Path(sourceFolderUri)).filter(!_.isDirectory).map(_.path) //filter is to avoid copying folders (folders will get created where copying files). Caveat: empty folders will not be copied
    val rspath = getRelativePath(sourceFolderUri)
    val rtpath = getRelativePath(targetLocationUri)
    val targetFileList = sourceFileList.map(_.replaceAll(sourceFolderUri, targetLocationUri)) //uri to work on differnt fikle systems
    println(targetFileList.head)
    val paths = sourceFileList.zip(targetFileList).map(x => Paths(x._1, x._2))

    println(paths.head)
    copyFiles(sourceFolderUri, targetLocationUri, paths, partitionCount)

  }


  /*
    def moveFolderKeepAcls(sourceFolderUri: String, targetFolderUri: String, keepSourceFolder: Boolean = false)
                          (implicit conf: Configuration): Boolean = {
      println("Moving folders: " + sourceFolderUri + "  ==>>  " + targetFolderUri)
      checkIfFsIsTheSame(sourceFolderUri, targetFolderUri)
      val srcAcls = AclManager.getAclEntries(sourceFolderUri)
      val trgAcls = AclManager.getAclEntries(targetFolderUri)
      val fs = getFileSystem(conf, sourceFolderUri)
      val srcRelPath = getRelativePath(sourceFolderUri)
      val trgRelPath = getRelativePath(targetFolderUri)

      if (doesMoveLookSafe(fs, srcRelPath, trgRelPath)) {
        println("Deleting target folder")
        if (fs.exists(new Path(trgRelPath)))
          if (!fs.delete(new Path(trgRelPath), true)) throw new Exception("Cannot delete folder " + targetFolderUri)
        println("Moving folder " + srcRelPath + " ==>> " + trgRelPath)
        if (!fs.rename(new Path(srcRelPath), new Path(trgRelPath))) throw new Exception("Move of folder " + srcRelPath + " ==>> " + trgRelPath + " FAILED!")
        AclManager.resetAclEntries(targetFolderUri, trgAcls)
        if (fs.mkdirs(new Path(srcRelPath)))
          AclManager.resetAclEntries(sourceFolderUri, srcAcls)
        else
          println("Could not create folder " + sourceFolderUri)
        true //move successful although source folder could not be recreated
      } else
        false
    } */


  def moveFolderContent(sourceFolderUri: String, targetLocationUri: String, keepSourceFolder: Boolean = false, partitionCount: Int = 192)
                       (implicit spark: SparkSession, confEx: Configuration): Array[FSOperationResult] = {
    println("Moving folders content: " + sourceFolderUri + "  ==>>  " + targetLocationUri)
    checkIfFsIsTheSame(sourceFolderUri, targetLocationUri)
    val fs = getFileSystem(confEx, sourceFolderUri)

    if (!doesMoveLookSafe(fs, getRelativePath(sourceFolderUri), getRelativePath(targetLocationUri)))
      return Array()
    //delete target folder
    if (fs.exists(new Path(getRelativePath(targetLocationUri)))) //todo test if it works
      deleteAllChildObjects(targetLocationUri)
    else
      fs.mkdirs(new Path(targetLocationUri))

    val sourceFileList = fs.listStatus(new Path(sourceFolderUri)).map(x => x.getPath.toString)
    val targetFileList = sourceFileList.map(_.replaceAll(sourceFolderUri, targetLocationUri))
    val paths = sourceFileList.zip(targetFileList).map(x => Paths(x._1, x._2))

    val relativePaths = paths.map(x => Paths(getRelativePath(x.sourcePath), getRelativePath(x.targetPath)))
    println("ALL TO BE MOVED:")
    relativePaths.foreach(println)
    val res = moveFiles(relativePaths, sourceFolderUri, partitionCount)
    if (res.exists(!_.success))
      throw new Exception("Move operation was not successful. There are " + res.count(!_.success) + " of objects which was not moved... " +
        "The list of first 100 which was not moved is below...\n" +
        res.map(_.path).slice(0, 99).mkString("\n"))

    if (!keepSourceFolder)
      if (fs.delete(new Path(getRelativePath(sourceFolderUri)), true))
        println("WARNING: Folder " + sourceFolderUri + "could not be deleted and was left on storage device")
    res
  }


  def deleteAllChildObjects(folderAbsPath: String, parallelism: Int = 32)(implicit spark: SparkSession, confEx: Configuration): Unit = {
    val relPath = getRelativePath(folderAbsPath)
    val fs = getFileSystem(spark.sparkContext.hadoopConfiguration, folderAbsPath)
    val objectList = fs.listStatus(new Path(relPath))
    val paths = objectList.map(relPath + "/" + _.getPath.getName)

    val r = deletePaths(paths, folderAbsPath, parallelism)
    if (r.exists(!_.success)) throw new Exception("Deleting of some objects failed")
  }

  def copyFiles(sourceFolderUri: String, targetLocationUri: String, paths: Seq[Paths],
                partitionCount: Int, attempt: Int = 0)
               (implicit spark: SparkSession, confEx: Configuration): Array[FSOperationResult] = {
    val confsd = new ConfigSerDeser(confEx)
    val requestProcessed = spark.sparkContext.longAccumulator("CopyFilesProcessedCount")
    val res = spark.sparkContext.parallelize(paths, partitionCount).mapPartitions(x => {
      val conf = confsd.get()
      val srcFs = getFileSystem(conf, sourceFolderUri)
      val trgFs = getFileSystem(conf, targetLocationUri)
      x.map(paths => {
        requestProcessed.add(1)
        FSOperationResult(paths.sourcePath, fs.copySingleFile(conf, paths.sourcePath, paths.targetPath, srcFs, trgFs))
      })
    }).collect()
    val failed = res.filter(!_.success)
    println("Number of files copied properly: " + res.count(_.success))
    println("Files with errors: " + failed.length)
    if (failed.isEmpty) res
    else if (failed.length == paths.length || attempt > 4)
      throw new Exception("Copy of files did not succeed - please check why and here are some of them: \n" + failed.map(_.path).slice(0, 10).mkString("\n"))
    else {
      val failedPaths = paths.map(_.sourcePath).filter(x => failed.map(_.path).contains(x))
      val pathsForReprocessing = paths.filter(x => failedPaths.contains(x.sourcePath))
      println("Reprocessing " + failedPaths.length + " of failed paths...")
      res.filter(_.success) ++ copyFiles(sourceFolderUri, targetLocationUri, pathsForReprocessing, partitionCount, attempt + 1)
    }
  }

  def deletePaths(relativePaths: Seq[String], absFolderUri: String, partitionCount: Int = 32, attempt: Int = 0)
                 (implicit spark: SparkSession, confEx: Configuration): Array[FSOperationResult] = {
    val sdConf = new ConfigSerDeser(confEx)
    println("absFolderUri: " + absFolderUri)
    println("deleting paths count:" + relativePaths.size)
    println("First 20 files for deletion:")
    relativePaths.slice(0, 20).foreach(println)
    val res = spark.sparkContext.parallelize(relativePaths, partitionCount).mapPartitions(pathsPart => {
      val conf = sdConf.get()
      val fs = getFileSystem(conf, absFolderUri)
      pathsPart.map(path => Future {
        FSOperationResult(path, fs.delete(new Path(path), true))
      }).map(x => Await.result(x, 120.seconds))
    }).collect()

    val failed = res.filter(!_.success)
    println("Number of paths deleted properly: " + res.count(_.success == true))
    println("Files with errors: " + res.count(!_.success))
    if (failed.isEmpty) res
    else if (failed.length == relativePaths.length || attempt > 4)
      throw new Exception("Delete of some paths did not succeed - please check why and here are some of them: \n" + failed.map(_.path).slice(0, 10).mkString("\n"))
    else
      res.filter(_.success) ++ deletePaths(relativePaths.filter(x => failed.contains(x)), absFolderUri, partitionCount, attempt + 1)

  }

  private def moveFiles(relativePaths: Seq[Paths], sourceFolderUri: String, partitionCount: Int = 32, attempt: Int = 0)
                       (implicit spark: SparkSession, confEx: Configuration): Array[FSOperationResult] = {
    println("Starting moveFiles. Paths to be moved: " + relativePaths.size)

    val requestProcessed = spark.sparkContext.longAccumulator("MoveFilesProcessedCount")
    val sdConf = new ConfigSerDeser(confEx)
    val res = spark.sparkContext.parallelize(relativePaths, partitionCount).mapPartitions(x => {
      val conf = sdConf.get()
      val srcFs = getFileSystem(conf, sourceFolderUri) //move can be done only within single fs, which makes sense :)
      x.map(paths => {
        requestProcessed.add(1)
        println("Executor paths: " + paths)
        Future(paths, srcFs.rename(new Path(paths.sourcePath), new Path(paths.targetPath))) //todo this fails if folder structure for the file does not exist
      })
    }).map(x => Await.result(x, 120.seconds)).map(x => FSOperationResult(x._1.sourcePath, x._2)).collect()
    println("Number of files moved properly: " + res.count(_.success))
    println("Files with errors: " + res.count(!_.success))
    val failed = res.filter(!_.success)

    if (failed.isEmpty) res
    else if (failed.length == relativePaths.length || attempt > 4)
      throw new Exception("Move of files did not succeed - please check why and here are some of them: \n" + failed.map(_.path).slice(0, 10).mkString("\n"))
    else {
      val failedPaths = relativePaths.map(_.sourcePath).filter(x => failed.map(_.path).contains(x))
      val pathsForReprocessing = relativePaths.filter(x => failedPaths.contains(x.sourcePath))
      println("Reprocessing " + failedPaths.length + " of failed paths...")
      res.filter(_.success) ++ moveFiles(pathsForReprocessing, sourceFolderUri, partitionCount, attempt + 1)
    }
  }


}

/*
*/
