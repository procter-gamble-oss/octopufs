package com.pg.bigdata.utils

import java.util.concurrent.Executors

import com.pg.bigdata.utils.Assistant._
import com.pg.bigdata.utils.fs.{FSOperationResult, Paths, _}
import com.pg.bigdata.utils.helpers.ConfigSerDeser
import com.pg.bigdata.utils.metastore._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

//val magicPrefix = ".dfs.core.windows.net"

object Promotor extends Serializable {

  def copyFilesBetweenTables(sourceTableName: String, targetTableName: String, partitionCnt: Int)
                            (implicit spark: SparkSession, confEx: Configuration): Array[FSOperationResult] = {
    val db = spark.catalog.currentDatabase
    copyFilesBetweenTables(db, sourceTableName, db, targetTableName, partitionCnt)
  }

  def copyFilesBetweenTables(sourceDbName: String, sourceTableName: String, targetDbName: String, targetTableName: String, partitionCount: Int = 192)
                            (implicit spark: SparkSession, confEx: Configuration): Array[FSOperationResult] = {
    val paths = getTablesPathsList(sourceDbName, sourceTableName, targetDbName, targetTableName) //.take(5)
    if (paths.isEmpty) {
      //throw new Exception("No files to be copied")
      println("No files to be copied for table  " + sourceDbName + "." + sourceTableName)
      Array[FSOperationResult]()
    }

    val srcLoc = getTableLocation(sourceDbName, sourceTableName)
    val trgLoc = getTableLocation(targetDbName, targetTableName)
    copyFolder(srcLoc, trgLoc, partitionCount)
  }

  def copyTablePartitions(sourceTableName: String, targetTableName: String, matchStringPartitions: Seq[String],
                          partitionCount: Int = 192)
                         (implicit spark: SparkSession, confEx: Configuration): Array[FSOperationResult] = {
    copyTablePartitions(spark.catalog.currentDatabase, sourceTableName, spark.catalog.currentDatabase,
      targetTableName, matchStringPartitions, partitionCount)
  }

  //if target folder exists, it will be deleted first

  def copyOverwritePartitions(sourceTableName: String, targetTableName: String, matchStringPartitions: Seq[String], partitionCount: Int)
                             (implicit spark: SparkSession, confEx: Configuration): Array[FSOperationResult] = {
    val db = spark.catalog.currentDatabase
    deleteTablePartitions(db, targetTableName, matchStringPartitions) //todo error handling or exception
    copyTablePartitions(db, sourceTableName, db, targetTableName, matchStringPartitions, partitionCount) //todo rethink approach to partition count
  }

  def copyOverwriteTable(sourceTableName: String, targetTableName: String, partitionCount: Int)
                        (implicit spark: SparkSession, confEx: Configuration): Array[FSOperationResult] = {
    val db = spark.catalog.currentDatabase

    copyOverwriteTable(db, sourceTableName, db, targetTableName, partitionCount) //todo rethink approach to partition count
  }

  def copyOverwriteTable(sourceDbName: String, sourceTableName: String, targetDbName: String, targetTableName: String, partitionCount: Int = 192)
                        (implicit spark: SparkSession, confEx: Configuration): Array[FSOperationResult] = {
    val db = spark.catalog.currentDatabase
    val trgLoc = getTableLocation(targetDbName, targetTableName)
    deleteAllChildObjects(trgLoc)
    val res = copyFilesBetweenTables(sourceDbName, sourceTableName, targetDbName, targetTableName, partitionCount) //todo rethink approach to partition count
    refreshMetadata(targetDbName, targetTableName)
    res
  }

  def copyTablePartitions(sourceDbName: String, sourceTableName: String, targetDbName: String, targetTableName: String, matchStringPartitions: Seq[String],
                          partitionCount: Int)
                         (implicit spark: SparkSession, confEx: Configuration): Array[FSOperationResult] = {
    val paths = filterPartitions(sourceDbName, sourceTableName, matchStringPartitions)
    val sourceAbsTblLoc = getTableLocation(sourceDbName, sourceTableName)
    val targetAbsTblLoc = getTableLocation(targetDbName, targetTableName)
    val sourceTargetPaths = paths.map(x => Paths(x, x.replace(sourceAbsTblLoc, targetAbsTblLoc)))
    println("Partitions of table " + sourceDbName + "." + sourceTableName + " which are going to be copied to " + targetDbName + "." + targetTableName + ":")
    sourceTargetPaths.foreach(x => println(x))
    val resultDfs = sourceTargetPaths.map(p => copyFolder(p.sourcePath, p.targetPath, partitionCount))
    val out = resultDfs.reduce(_ union _)
    refreshMetadata(targetDbName, targetTableName)
    out
  }

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

  def moveFolderContentLocal(sourceFolderUri: String, targetLocationUri: String, keepSourceFolder: Boolean = false, numOfThreads: Int = 192)
                       (implicit conf: Configuration): Array[FSOperationResult] = {
    println("Moving folders content: " + sourceFolderUri + "  ==>>  " + targetLocationUri)
    checkIfFsIsTheSame(sourceFolderUri, targetLocationUri)
    val fs = getFileSystem(conf, sourceFolderUri)

    if (!doesMoveLookSafe(fs, getRelativePath(sourceFolderUri), getRelativePath(targetLocationUri)))
      return Array()
    //delete target folder
    if (fs.exists(new Path(getRelativePath(targetLocationUri)))) //todo test if it works
      deleteAllChildObjectsLocal(targetLocationUri)
    else
      fs.mkdirs(new Path(targetLocationUri))

    val sourceFileList = fs.listStatus(new Path(sourceFolderUri)).map(x => x.getPath.toString)
    val targetFileList = sourceFileList.map(_.replaceAll(sourceFolderUri, targetLocationUri))
    val paths = sourceFileList.zip(targetFileList).map(x => Paths(x._1, x._2))

    val relativePaths = paths.map(x => Paths(getRelativePath(x.sourcePath), getRelativePath(x.targetPath)))
    println("ALL TO BE MOVED:")
    relativePaths.foreach(println)
    val res = moveFilesLocal(relativePaths, sourceFolderUri, numOfThreads)(conf)
    if (res.exists(!_.success))
      throw new Exception("Move operation was not successful. There are " + res.count(!_.success) + " of objects which was not moved... " +
        "The list of first 100 which was not moved is below...\n" +
        res.map(_.path).slice(0, 99).mkString("\n"))

    if (!keepSourceFolder)
      if (fs.delete(new Path(getRelativePath(sourceFolderUri)), true))
        println("WARNING: Folder " + sourceFolderUri + "could not be deleted and was left on storage device")
    res.toArray
  }

  def copyOverwritePartitions(sourceDbName: String, sourceTableName: String, targetDbName: String, targetTableName: String, matchStringPartitions: Seq[String],
                              partitionCount: Int = 192)
                             (implicit spark: SparkSession, confEx: Configuration): Array[FSOperationResult] = {
    deleteTablePartitions(targetDbName, targetTableName, matchStringPartitions)
    copyTablePartitions(sourceDbName, sourceTableName, targetDbName, targetTableName, matchStringPartitions, partitionCount)
  }

  def moveTablePartitions(sourceTableName: String, targetTableName: String, matchStringPartitions: Seq[String],
                          moveContentOnly: Boolean, partitionCount: Int)
                         (implicit spark: SparkSession, confEx: Configuration): Array[FSOperationResult] = {
    val db = spark.catalog.currentDatabase
    moveTablePartitions(db, sourceTableName, db, targetTableName, matchStringPartitions, moveContentOnly, partitionCount)
  }

  def moveTablePartitions(sourceDbName: String, sourceTableName: String, targetDbName: String, targetTableName: String,
                          matchStringPartitions: Seq[String] = Seq(), moveContentOnly: Boolean = false,
                          partitionCount: Int)
                         (implicit spark: SparkSession, confEx: Configuration): Array[FSOperationResult] = {
    val paths = filterPartitions(sourceDbName, sourceTableName, matchStringPartitions)
    val sourceAbsTblLoc = getTableLocation(sourceDbName, sourceTableName)
    val targetAbsTblLoc = getTableLocation(targetDbName, targetTableName)
    val sourceTargetPaths = paths.map(x => Paths(x, x.replace(sourceAbsTblLoc, targetAbsTblLoc)))
    println("Partitions of table " + sourceDbName + "." + sourceTableName + " which are going to be moved to " + targetDbName + "." + targetTableName + ":")
    //todo add check for empty list
    sourceTargetPaths.foreach(x => println(x))
    val resultDfs = sourceTargetPaths.map(p => {
      moveFolderContent(p.sourcePath, p.targetPath, moveContentOnly, partitionCount)
    }).reduce(_ union _)

    refreshMetadata(sourceDbName, sourceTableName)
    refreshMetadata(targetDbName, targetTableName)

    resultDfs
  }

  def checkIfFsIsTheSame(targetLocationUri: String, sourceFolderUri: String): Unit = {
    if (getFileSystemPrefix(targetLocationUri) + getContainerName(targetLocationUri) != getFileSystemPrefix(sourceFolderUri) + getContainerName(sourceFolderUri))
      throw new Exception("Cannot move files between 2 different filesystems. Use copy instead")
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

  private def doesMoveLookSafe(fs: FileSystem, sourceRelPath: String, targetRelPath: String): Boolean = {
    if (!fs.exists(new Path(sourceRelPath))) throw new Exception("Source folder " + sourceRelPath + " does not exist")
    val src = fs.listStatus(new Path(sourceRelPath))
    val trg = if (fs.exists(new Path(targetRelPath)))
      fs.listStatus(new Path(targetRelPath))
    else return true

    if (src.nonEmpty || (src.isEmpty && trg.isEmpty)) true
    else {
      println("Looks like your source folder " + sourceRelPath + " is empty, but your target folder " + targetRelPath +
        " is not. Skipping the move to avoid harmful folder move (assuming it is rerun)")
      false
    }
  }

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

  def moveFilesLocal(relativePaths: Array[Paths], sourceFolderUri: String, numOfThreads: Int = 32, timeoutMins: Int = 10, attempt: Int = 0)
                    (implicit conf: Configuration): Seq[FSOperationResult] = {
    println("Starting moveFiles. Paths to be moved: " + relativePaths.size)
    implicit val pool = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(numOfThreads))
    val fs = getFileSystem(conf, sourceFolderUri)
    val res = relativePaths.map(x => Future {
      print(".")
      FSOperationResult(x.sourcePath, fs.rename(new Path(x.sourcePath), new Path(x.targetPath)))
    }(pool)).map(x => Await.result(x, timeoutMins.minutes))

    println("Number of files moved properly: " + res.count(_.success))
    println("Files with errors: " + res.count(!_.success))
    val failed = res.filter(!_.success)

    if (failed.isEmpty) res
    else if (failed.length == relativePaths.length || attempt > 4)
      throw new Exception("Move of files did not succeed - please check why and here are some of them: \n" + failed.map(_.path).slice(0, 10).mkString("\n"))
    else
      res.filter(_.success) ++ moveFilesLocal(relativePaths.filter(x => failed.contains(x)), sourceFolderUri, numOfThreads, timeoutMins, attempt + 1)(conf)
  }

  def deletePathsLocal(fs: FileSystem, paths: Array[String],timeoutMin: Int = 10, parallelism: Int = 32, attempt: Int = 0)
                      (implicit confEx: Configuration): Array[FSOperationResult] = {
    val res = paths.map( x =>
      Future {
        FSOperationResult(x, fs.delete(new Path(x), true))
      }).map(x => Await.result(x, timeoutMin.minutes))

    val failed = res.filter(!_.success)
    println("Number of paths deleted properly: " + res.count(_.success == true))
    println("Files with errors: " + res.count(!_.success))
    if (failed.isEmpty) res
    else if (failed.length == paths.length || attempt > 4)
      throw new Exception("Delete of some paths did not succeed - please check why and here are some of them: \n" + failed.map(_.path).slice(0, 10).mkString("\n"))
    else
      res.filter(_.success) ++ deletePathsLocal(fs, paths.filter(x => failed.contains(x)), timeoutMin, parallelism, attempt + 1)
  }

  def deleteAllChildObjectsLocal(folderAbsPath: String, timeoutMin: Int = 10, parallelism: Int = 32, attempt: Int = 0)(implicit confEx: Configuration): Unit = {
    val relPath = getRelativePath(folderAbsPath)
    val fs = getFileSystem(confEx, folderAbsPath)
    val objectList = fs.listStatus(new Path(relPath))
    val paths = objectList.map(relPath + "/" + _.getPath.getName)

    println("absFolderUri: " + folderAbsPath)
    println("deleting paths count:" + paths.size)
    println("First 20 files for deletion:")
    paths.slice(0, 20).foreach(println)
    deletePathsLocal(fs,paths,timeoutMin,parallelism,attempt)
  }

  def deleteAllChildObjects(folderAbsPath: String, parallelism: Int = 32)(implicit spark: SparkSession, confEx: Configuration): Unit = {
    val relPath = getRelativePath(folderAbsPath)
    val fs = getFileSystem(spark.sparkContext.hadoopConfiguration, folderAbsPath)
    val objectList = fs.listStatus(new Path(relPath))
    val paths = objectList.map(relPath + "/" + _.getPath.getName)

    val r = deletePaths(paths, folderAbsPath, parallelism)
    if (r.exists(!_.success)) throw new Exception("Deleting of some objects failed")
  }

  private def copyFiles(sourceFolderUri: String, targetLocationUri: String, p: Seq[Paths],
                        partitionCount: Int, attempt: Int = 0)
                       (implicit spark: SparkSession, confEx: Configuration): Array[FSOperationResult] = {
    val confsd = new ConfigSerDeser(confEx)
    val requestProcessed = spark.sparkContext.longAccumulator("CopyFilesProcessedCount")
    val res = spark.sparkContext.parallelize(p, partitionCount).mapPartitions(x => {
      val conf = confsd.get()
      val srcFs = getFileSystem(conf, sourceFolderUri)
      val trgFs = getFileSystem(conf, targetLocationUri)
      x.map(paths => {
        requestProcessed.add(1)
        FSOperationResult(paths.sourcePath, Promotor.copySingleFile(conf, paths.sourcePath, paths.targetPath, srcFs, trgFs))
      })
    }).collect()
    val failed = res.filter(!_.success)
    println("Number of files copied properly: " + res.count(_.success))
    println("Files with errors: " + failed.length)
    if (failed.isEmpty) res
    else if (failed.length == p.length || attempt > 4)
      throw new Exception("Copy of files did not succeed - please check why and here are some of them: \n" + failed.map(_.path).slice(0, 10).mkString("\n"))
    else
      res.filter(_.success) ++ copyFiles(sourceFolderUri, targetLocationUri, p.filter(x => failed.contains(x)), partitionCount, attempt + 1)
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
    else
      res.filter(_.success) ++ moveFiles(relativePaths.filter(x => failed.contains(x)), sourceFolderUri, partitionCount, attempt + 1)
  }

  def deleteTablePartitions(db: String, tableName: String, matchStringPartitions: Seq[String], parallelism: Int = 32)(implicit spark: SparkSession, confEx: Configuration): Unit = {
    val paths = filterPartitions(db, tableName, matchStringPartitions).map(x => getRelativePath(x))
    val absTblLoc = getTableLocation(db, tableName)
    println("Partitions of table " + db + "." + tableName + " which are going to be deleted:")
    paths.foreach(println)
    val r = deletePaths(paths, absTblLoc, parallelism)
    if (r.exists(!_.success)) throw new Exception("Deleting of some partitions failed")
    refreshMetadata(db, tableName)
  }

  private def copySingleFile(hadoopConf: Configuration, sourcePath: String, targetPath: String, sourceFileSystem: FileSystem, targetFileSystem: FileSystem,
                             overwrite: Boolean = true, deleteSource: Boolean = false): Boolean = {
    println(sourcePath + " => " + targetPath)
    val srcPath = new Path(sourcePath)
    val destPath = new Path(targetPath)
    org.apache.hadoop.fs.FileUtil.copy(sourceFileSystem, srcPath, targetFileSystem, destPath, deleteSource, overwrite, hadoopConf)
  }

  def moveFilesBetweenTables(sourceTableName: String, targetTableName: String, partitionCnt: Int)
                            (implicit spark: SparkSession, conf1: Configuration): Array[FSOperationResult] = {
    val db = spark.catalog.currentDatabase
    moveFilesBetweenTables(db, sourceTableName, db, targetTableName, partitionCnt)
  }

  private def deletePaths(relativePaths: Seq[String], absFolderUri: String, partitionCount: Int = 32, attempt: Int = 0)
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


  def moveFilesBetweenTables(sourceDbName: String, sourceTableName: String, targetDbName: String, targetTableName: String, partitionCount: Int = 192)
                            (implicit spark: SparkSession, confEx: Configuration): Array[FSOperationResult] = {
    val srcLoc = getTableLocation(sourceDbName, sourceTableName)
    val trgLoc = getTableLocation(targetDbName, targetTableName)
    checkIfFsIsTheSame(srcLoc, trgLoc)
    val paths = getTablesPathsList(sourceDbName, sourceTableName, targetDbName, targetTableName) //.take(5)
    val targetFilesToBeDeleted = getListOfTableFiles(targetDbName, targetTableName)
    if (paths.isEmpty) {
      //throw new Exception("No files to be moved for path " + srcLoc)
      println("No files to be moved from path " + srcLoc)
      Array[FSOperationResult]()
    }
    println(paths(0))
    println("Files to be moved: " + paths.length)
    val relativePaths = paths.map(x => Paths(x.sourcePath, x.targetPath))
    val res = moveFolderContent(srcLoc, trgLoc, keepSourceFolder = true)
    refreshMetadata(sourceDbName, sourceTableName)
    refreshMetadata(targetDbName, targetTableName)
    res
  }

}

/*
*/
