package com.pg.bigdata.utils

import com.pg.bigdata.utils.Assistant._
import com.pg.bigdata.utils.fs.{DistributedExecution, FsOperationResult, Paths, _}
import com.pg.bigdata.utils.metastore._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

import scala.concurrent.ExecutionContext
import com.pg.bigdata.utils.helpers.implicits._
import scala.concurrent.forkjoin.ForkJoinPool

//val magicPrefix = ".dfs.core.windows.net"

object Promotor extends Serializable {

  def copyFilesBetweenTables(sourceTableName: String, targetTableName: String, partitionCnt: Int)
                            (implicit spark: SparkSession, confEx: Configuration): Array[FsOperationResult] = {
    val db = spark.catalog.currentDatabase
    copyFilesBetweenTables(db, sourceTableName, db, targetTableName, partitionCnt)
  }

  def copyFilesBetweenTables(sourceDbName: String, sourceTableName: String, targetDbName: String, targetTableName: String, partitionCount: Int = 192)
                            (implicit spark: SparkSession, confEx: Configuration): Array[FsOperationResult] = {
    val paths = getTablesPathsList(sourceDbName, sourceTableName, targetDbName, targetTableName) //.take(5)
    if (paths.isEmpty) {
      //throw new Exception("No files to be copied")
      println("No files to be copied for table  " + sourceDbName + "." + sourceTableName)
      Array[FsOperationResult]()
    }

    val srcLoc = getTableLocation(sourceDbName, sourceTableName)
    val trgLoc = getTableLocation(targetDbName, targetTableName)
    DistributedExecution.copyFolder(srcLoc, trgLoc, partitionCount)
  }

  def copyTablePartitions(sourceTableName: String, targetTableName: String, matchStringPartitions: Seq[String],
                          partitionCount: Int = 192)
                         (implicit spark: SparkSession, confEx: Configuration): Array[FsOperationResult] = {
    copyTablePartitions(spark.catalog.currentDatabase, sourceTableName, spark.catalog.currentDatabase,
      targetTableName, matchStringPartitions, partitionCount)
  }

  //if target folder exists, it will be deleted first

  def copyOverwritePartitions(sourceTableName: String, targetTableName: String, matchStringPartitions: Seq[String], partitionCount: Int)
                             (implicit spark: SparkSession, confEx: Configuration): Array[FsOperationResult] = {
    val db = spark.catalog.currentDatabase
    deleteTablePartitions(db, targetTableName, matchStringPartitions) //todo error handling or exception
    copyTablePartitions(db, sourceTableName, db, targetTableName, matchStringPartitions, partitionCount) //todo rethink approach to partition count
  }

  def copyOverwriteTable(sourceTableName: String, targetTableName: String, partitionCount: Int)
                        (implicit spark: SparkSession, confEx: Configuration): Array[FsOperationResult] = {
    val db = spark.catalog.currentDatabase

    copyOverwriteTable(db, sourceTableName, db, targetTableName, partitionCount) //todo rethink approach to partition count
  }

  def copyOverwriteTable(sourceDbName: String, sourceTableName: String, targetDbName: String, targetTableName: String, partitionCount: Int = 192)
                        (implicit spark: SparkSession, confEx: Configuration): Array[FsOperationResult] = {
    val trgLoc = getTableLocation(targetDbName, targetTableName)
    LocalExecution.deleteFolder(trgLoc,true)
    val res = copyFilesBetweenTables(sourceDbName, sourceTableName, targetDbName, targetTableName, partitionCount) //todo rethink approach to partition count
    refreshMetadata(targetDbName, targetTableName)
    res
  }

  def copyTablePartitions(sourceDbName: String, sourceTableName: String, targetDbName: String, targetTableName: String, matchStringPartitions: Seq[String],
                          partitionCount: Int)
                         (implicit spark: SparkSession, confEx: Configuration): Array[FsOperationResult] = {
    val paths = filterPartitions(sourceDbName, sourceTableName, matchStringPartitions)
    if (paths.isEmpty)
      throw new Exception("There are no files to be copied. Please check your input parameters or table content")

    println("Partitions to be copied: " + paths.mkString("\n"))
    val sourceAbsTblLoc = getTableLocation(sourceDbName, sourceTableName)
    val targetAbsTblLoc = getTableLocation(targetDbName, targetTableName)
    val fs = getFileSystem(confEx, sourceAbsTblLoc)



    val allSourceFiles = paths.map(x => listLevel(fs, Array(new Path(x))).filter(!_.isDirectory)).reduce(_ union _)
    println("Total number of files to be copied: " + allSourceFiles.length)
    val sourceTargetPaths = allSourceFiles.map(x => Paths(x.path, x.path.replace(sourceAbsTblLoc, targetAbsTblLoc)))
    println("Partitions of table " + sourceDbName + "." + sourceTableName + " which are going to be copied to " + targetDbName + "." + targetTableName + ":")
    sourceTargetPaths.slice(0, 10).foreach(x => println(x))
    val res = DistributedExecution.copyFiles(sourceTargetPaths, partitionCount)
    refreshMetadata(targetDbName, targetTableName)
    res
  }

  def copyOverwritePartitions(sourceDbName: String, sourceTableName: String, targetDbName: String, targetTableName: String, matchStringPartitions: Seq[String],
                              partitionCount: Int = 192)
                             (implicit spark: SparkSession, confEx: Configuration): Array[FsOperationResult] = {
    deleteTablePartitions(targetDbName, targetTableName, matchStringPartitions)
    copyTablePartitions(sourceDbName, sourceTableName, targetDbName, targetTableName, matchStringPartitions, partitionCount)
  }

  def moveTablePartitions(sourceTableName: String, targetTableName: String, matchStringPartitions: Seq[String],
                          moveContentOnly: Boolean, partitionCount: Int)
                         (implicit spark: SparkSession, confEx: Configuration): Array[FsOperationResult] = {
    val db = spark.catalog.currentDatabase
    moveTablePartitionFolders(db, sourceTableName, db, targetTableName, matchStringPartitions, moveContentOnly, partitionCount)
  }


  def moveTablePartitionFolders(sourceDbName: String, sourceTableName: String, targetDbName: String, targetTableName: String,
                                matchStringPartitions: Seq[String] = Seq(), moveContentOnly: Boolean = false,
                                numOfThreads: Int = 1000, timeoutMin: Int = 10)
                               (implicit spark: SparkSession, conf: Configuration): Array[FsOperationResult] = {
    val backupPrefix = "promotor_backup_"
    //val transaction = SafetyFuse()
    val partitionFoldersUriPaths = filterPartitions(sourceDbName, sourceTableName, matchStringPartitions)
    val sourceAbsTblLoc = getTableLocation(sourceDbName, sourceTableName)
    val targetAbsTblLoc = getTableLocation(targetDbName, targetTableName)
    val sourceTargetUriPaths = partitionFoldersUriPaths.map(x => Paths(x, x.replace(sourceAbsTblLoc, targetAbsTblLoc)))
    implicit val srcFs = getFileSystem(conf, sourceAbsTblLoc)
    val trgFs = getFileSystem(conf, targetAbsTblLoc)

    checkIfFsIsTheSame(srcFs, trgFs) //throws exception in case of discrepancy

    if (partitionFoldersUriPaths.isEmpty) {
      println("There is nothing to be moved (source " + sourceAbsTblLoc + "is empty)")
      return Array[FsOperationResult]()
    }

    println("Partitions of table " + sourceDbName + "." + sourceTableName + " which are going to be moved to " + targetDbName + "." + targetTableName + ":")

    //check if it is safe to move folders
    sourceTargetUriPaths.foreach(x =>
      if (!doesMoveLookSafe(srcFs, x.sourcePath, x.targetPath)) {
        throw new Exception("It looks like there is a path in source " + x.sourcePath + " which is empty, but target folder " + x.targetPath + " is not. " +
          "Promotor tries to protect you from unintentional deletion of your data in the target. If you want to avoid this exception, " +
          "place at least one empty file in the source folder to avoid run interruption")
      }
    )

    val existingTargetFolders = partitionFoldersUriPaths.map(_.replace(sourceAbsTblLoc, targetAbsTblLoc)).
      filter(folder => srcFs.exists(new Path(folder)))
    //make backup
    val transaction = new SafetyFuse(sourceAbsTblLoc)
    if (!transaction.isInProgress()) {
      transaction.startTransaction()
      println("Deleting targets... (showing 10 sample paths")
      existingTargetFolders.slice(0, 10).foreach(println)
      LocalExecution.deletePaths(srcFs, existingTargetFolders, timeoutMin, numOfThreads)
    }

    println("Now moving source... Showing first 10 paths")
    sourceTargetUriPaths.slice(0, 10).foreach(println)
    val res = LocalExecution.moveFiles(sourceTargetUriPaths, numOfThreads, timeoutMin)

    refreshMetadata(sourceDbName, sourceTableName)
    refreshMetadata(targetDbName, targetTableName)
    transaction.endTransaction()
    res
  }

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

  def deleteTablePartitions(db: String, tableName: String, matchStringPartitions: Seq[String], parallelism: Int = 32)(implicit spark: SparkSession, confEx: Configuration): Unit = {
    val paths = filterPartitions(db, tableName, matchStringPartitions)
    val absTblLoc = getTableLocation(db, tableName)
    println("Partitions of table " + db + "." + tableName + " which are going to be deleted:")
    paths.foreach(println)
    LocalExecution.deletePaths(getFileSystem(confEx, absTblLoc), paths, 10, parallelism)
    refreshMetadata(db, tableName)
  }

  def moveFilesBetweenTables(sourceTableName: String, targetTableName: String, partitionCnt: Int)
                            (implicit spark: SparkSession, conf1: Configuration): Array[FsOperationResult] = {
    val db = spark.catalog.currentDatabase
    moveFilesBetweenTables(db, sourceTableName, db, targetTableName, partitionCnt)
  }


  def moveFilesBetweenTables(sourceDbName: String, sourceTableName: String, targetDbName: String, targetTableName: String,
                             partitionCount: Int = 1000, timeoutMin: Int = 10)
                            (implicit spark: SparkSession, confEx: Configuration): Array[FsOperationResult] = {
    val srcLoc = getTableLocation(sourceDbName, sourceTableName)
    val trgLoc = getTableLocation(targetDbName, targetTableName)
    val res = LocalExecution.moveFolderContent(srcLoc, trgLoc, keepSourceFolder = true)
    refreshMetadata(sourceDbName, sourceTableName)
    refreshMetadata(targetDbName, targetTableName)
    res
  }

}

/*
*/
