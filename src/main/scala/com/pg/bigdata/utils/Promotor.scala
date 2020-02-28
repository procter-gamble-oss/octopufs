package com.pg.bigdata.utils

import com.pg.bigdata.utils.Assistant._
import com.pg.bigdata.utils.fs.{FSOperationResult, Paths, _}
import com.pg.bigdata.utils.helpers.ConfigSerDeser
import com.pg.bigdata.utils.metastore._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

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
    if (paths.isEmpty)
      throw new Exception("No files to be copied")

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
    refreshMetadata(targetDbName,targetTableName)
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
    val out = resultDfs.reduce(_ union _) //todo add verification if all files were copied correctly
    refreshMetadata(targetDbName, targetTableName)
    out
  }

  def copyFolder(sourceFolderUri: String, targetLocationUri: String, partitionCount: Int = 192)(implicit spark: SparkSession, confEx: Configuration): Array[FSOperationResult] = {
    val srcFs = getFileSystem(confEx, sourceFolderUri)
    val sourceFileList = listRecursively(srcFs, new Path(sourceFolderUri)).filter(!_.isDirectory).map(_.path) //filter is to avoid copying folders (folders will get created where copying files). Caveat: empty folders will not be copied
    val rspath = getRelativePath(sourceFolderUri)
    val rtpath = getRelativePath(targetLocationUri)
    val targetFileList = sourceFileList.map(_.replaceAll(rspath, rtpath))
    println(targetFileList.head)
    val paths = sourceFileList.zip(targetFileList).map(x => Paths(x._1, x._2))

    println(paths.head)
    copyFiles(sourceFolderUri, targetLocationUri, paths, partitionCount)

  }

  private def copyFiles(sourceFolderUri: String, targetLocationUri: String, p: Seq[Paths],
                        partitionCount: Int)
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
    if(failed.nonEmpty)
      throw new Exception("Copy of files did not succeed - please check why and here are some of them: \n"+failed.map(_.path).slice(0,10).mkString("\n"))
    res
  }

  def copyOverwritePartitions(sourceDbName: String, sourceTableName: String, targetDbName: String, targetTableName: String, matchStringPartitions: Seq[String],
                              partitionCount: Int = 192)
                             (implicit spark: SparkSession, confEx: Configuration): Array[FSOperationResult] = {
    deleteTablePartitions(targetDbName, targetTableName, matchStringPartitions) //todo error handling or exception
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
      moveFolder(p.sourcePath, p.targetPath, moveContentOnly, partitionCount)
    }).reduce(_ union _)

    refreshMetadata(sourceDbName, sourceTableName)
    refreshMetadata(targetDbName, targetTableName)

    resultDfs
  }
  def checkIfFsIsTheSame(targetLocationUri: String, sourceFolderUri: String): Unit = {
    if (getFileSystemPrefix(targetLocationUri) + getContainerName(targetLocationUri) != getFileSystemPrefix(sourceFolderUri) + getContainerName(sourceFolderUri))
      throw new Exception("Cannot move files between 2 different filesystems. Use copy instead")
  }
  //todo zobaczyc jak sie zachowa gdy docelowy folder/pliki juz istnieje
  def moveFolder(sourceFolderUri: String, targetLocationUri: String, moveContentOnly: Boolean = false, partitionCount: Int = 192)
                (implicit spark: SparkSession, confEx: Configuration): Array[FSOperationResult] = {
    println("Moving folders content: " + sourceFolderUri + "  ==>>  " + targetLocationUri)
    checkIfFsIsTheSame(sourceFolderUri, targetLocationUri)
    val srcFs = getFileSystem(confEx, sourceFolderUri)
    val trgFs = getFileSystem(confEx, targetLocationUri)
    //delete target folder

    if (trgFs.exists(new Path(getRelativePath(targetLocationUri)))) //todo test if it works
      deleteAllChildObjects(targetLocationUri)
    else
      trgFs.mkdirs(new Path(targetLocationUri))

    val sourceFileList = srcFs.listStatus(new Path(sourceFolderUri)).map(x => x.getPath.toString)
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

    if (!moveContentOnly)
      if (srcFs.delete(new Path(getRelativePath(sourceFolderUri)), true))
        println("WARNING: Folder " + sourceFolderUri + "could not be deleted and was left on storage device")
    res
  }

  private def moveFiles(relativePaths: Seq[Paths], sourceFolderUri: String, partitionCount: Int = 32)
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
    }).map(x => Await.result(x, 10.seconds)).map(x => FSOperationResult(x._1.sourcePath, x._2)).collect()
    println("Number of files moved properly: " + res.count(_.success))
    println("Files with errors: " + res.count(!_.success))
    if(res.exists(_.success == false))
      throw new Exception("Move/rename of did not work for some files. Please check the reason or try rerunning the task")
    res
  }

  def deleteAllChildObjects(folderAbsPath: String)(implicit spark: SparkSession, confEx: Configuration): Unit = {
    val relPath = getRelativePath(folderAbsPath)
    val fs = getFileSystem(spark.sparkContext.hadoopConfiguration, folderAbsPath)
    val objectList = fs.listStatus(new Path(relPath))
    val paths = objectList.map(relPath + "/" + _.getPath.getName)

    val r = deletePaths(paths, folderAbsPath)
    if (r.exists(!_.success)) throw new Exception("Deleting of some objects failed")
  }

  def deleteTablePartitions(db: String, tableName: String, matchStringPartitions: Seq[String])(implicit spark: SparkSession, confEx: Configuration): Unit = {
    val paths = filterPartitions(db, tableName, matchStringPartitions).map(x => getRelativePath(x))
    val absTblLoc = getTableLocation(db, tableName)
    println("Partitions of table " + db + "." + tableName + " which are going to be deleted:")
    paths.foreach(println)
    val r = deletePaths(paths, absTblLoc)
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

  private def deletePaths(relativePaths: Seq[String], absFolderUri: String, partitionCount: Int = 32)
                         (implicit spark: SparkSession, confEx: Configuration) = {
    val sdConf = new ConfigSerDeser(confEx)
    println("absFolderUri: " + absFolderUri)
    println("deleting paths count:" + relativePaths.size)
    println("First 20 files for deletion:")
    relativePaths.slice(0,20).foreach(println)
    val res = spark.sparkContext.parallelize(relativePaths, partitionCount).mapPartitions(pathsPart => {
      val conf = sdConf.get()
      val fs = getFileSystem(conf, absFolderUri)
      pathsPart.map(path => Future {
        FSOperationResult(path, fs.delete(new Path(path), true))
      }).map(x => Await.result(x, 10.seconds))
    })

    val out = res.collect()
    println("Number of paths deleted properly: " + out.count(_.success == true))
    println("Files with errors: " + out.count(!_.success))
    if(out.exists(!_.success))
      throw new Exception("Delete did not work for some files. Please check the reason or try rerunning the task")
    out
  }

  def moveFilesBetweenTables(sourceDbName: String, sourceTableName: String, targetDbName: String, targetTableName: String, partitionCount: Int = 192)
                            (implicit spark: SparkSession, confEx: Configuration): Array[FSOperationResult] = {
    val srcLoc = getTableLocation(sourceDbName, sourceTableName)
    val trgLoc = getTableLocation(targetDbName, targetTableName)
    checkIfFsIsTheSame(srcLoc, trgLoc)
    val paths = getTablesPathsList(sourceDbName, sourceTableName, targetDbName, targetTableName) //.take(5)
    val targetFilesToBeDeleted = getListOfTableFiles(targetDbName, targetTableName)
    if (paths.isEmpty)
      throw new Exception("No files to be moved for path " + srcLoc)
    println(paths(0))
    println("Files to be moved: " + paths.length)
    val relativePaths = paths.map(x => Paths(x.sourcePath, x.targetPath))
    val res = moveFolder(srcLoc,trgLoc, moveContentOnly = true)
    refreshMetadata(sourceDbName, sourceTableName)
    refreshMetadata(targetDbName, targetTableName)
    res
  }

}

/*
*/
