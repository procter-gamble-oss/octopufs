package com.pg.bigdata.utils

import java.util.concurrent.Executors

import com.pg.bigdata.utils.Assistant._
import com.pg.bigdata.utils.fs.{FSOperationResult, Paths, _}
import com.pg.bigdata.utils.helpers.ConfigSerDeser
import com.pg.bigdata.utils.metastore._
import com.pg.bigdata.utils.fs.DistributedExecution
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
    DistributedExecution.copyFolder(srcLoc, trgLoc, partitionCount)
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
    val trgLoc = getTableLocation(targetDbName, targetTableName)
    DistributedExecution.deleteAllChildObjects(trgLoc)
    val res = copyFilesBetweenTables(sourceDbName, sourceTableName, targetDbName, targetTableName, partitionCount) //todo rethink approach to partition count
    refreshMetadata(targetDbName, targetTableName)
    res
  }

  def copyTablePartitions(sourceDbName: String, sourceTableName: String, targetDbName: String, targetTableName: String, matchStringPartitions: Seq[String],
                          partitionCount: Int)
                         (implicit spark: SparkSession, confEx: Configuration): Array[FSOperationResult] = {
    val paths = filterPartitions(sourceDbName, sourceTableName, matchStringPartitions)
    println("Partitions to be copied: "+paths.mkString("\n"))
    val sourceAbsTblLoc = getTableLocation(sourceDbName, sourceTableName)
    val targetAbsTblLoc = getTableLocation(targetDbName, targetTableName)
    val fs = getFileSystem(confEx,sourceAbsTblLoc)
   // val sourceTargetPaths = paths.map(x => Paths(x, x.replace(sourceAbsTblLoc, targetAbsTblLoc)))
    //println("Partitions of table " + sourceDbName + "." + sourceTableName + " which are going to be copied to " + targetDbName + "." + targetTableName + ":")
   // sourceTargetPaths.foreach(x => println(x))
    //val resultDfs = sourceTargetPaths.map(p => DistributedExecution.copyFolder(p.sourcePath, p.targetPath, partitionCount))
    val allSourceFiles = paths.map(x => listRecursively(fs, new Path(x)).filter(!_.isDirectory)).reduce(_ union _)
    println("Total number of files to be copied: "+allSourceFiles.length)
    val sourceTargetPaths = allSourceFiles.map(x => Paths(x.path, x.path.replace(sourceAbsTblLoc, targetAbsTblLoc)))
    println("Partitions of table " + sourceDbName + "." + sourceTableName + " which are going to be copied to " + targetDbName + "." + targetTableName + ":")
    sourceTargetPaths.slice(0,10).foreach(x => println(x))
    val res = DistributedExecution.copyFiles(sourceAbsTblLoc, targetAbsTblLoc,sourceTargetPaths,partitionCount)
    refreshMetadata(targetDbName, targetTableName)
    res
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
      DistributedExecution.moveFolderContent(p.sourcePath, p.targetPath, moveContentOnly, partitionCount)
    }).reduce(_ union _)

    refreshMetadata(sourceDbName, sourceTableName)
    refreshMetadata(targetDbName, targetTableName)

    resultDfs
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

  def deleteTablePartitions(db: String, tableName: String, matchStringPartitions: Seq[String], parallelism: Int = 32)(implicit spark: SparkSession, confEx: Configuration): Unit = {
    val paths = filterPartitions(db, tableName, matchStringPartitions).map(x => getRelativePath(x))
    val absTblLoc = getTableLocation(db, tableName)
    println("Partitions of table " + db + "." + tableName + " which are going to be deleted:")
    paths.foreach(println)
    val r = LocalExecution.deletePaths(getFileSystem(confEx, absTblLoc), paths, 10, parallelism)
    if (r.exists(!_.success)) throw new Exception("Deleting of some partitions failed")
    refreshMetadata(db, tableName)
  }

  def moveFilesBetweenTables(sourceTableName: String, targetTableName: String, partitionCnt: Int)
                            (implicit spark: SparkSession, conf1: Configuration): Array[FSOperationResult] = {
    val db = spark.catalog.currentDatabase
    moveFilesBetweenTables(db, sourceTableName, db, targetTableName, partitionCnt)
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
    val res = DistributedExecution.moveFolderContent(srcLoc, trgLoc, keepSourceFolder = true)
    refreshMetadata(sourceDbName, sourceTableName)
    refreshMetadata(targetDbName, targetTableName)
    res
  }

}

/*
*/
