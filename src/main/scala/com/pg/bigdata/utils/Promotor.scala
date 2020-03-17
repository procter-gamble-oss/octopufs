package com.pg.bigdata.utils

import com.pg.bigdata.utils.Assistant._
import com.pg.bigdata.utils.fs.{DistributedExecution, FSOperationResult, Paths, _}
import com.pg.bigdata.utils.metastore._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

import scala.concurrent.ExecutionContext.Implicits.global

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
    if (paths.isEmpty)
      throw new Exception("There are no files to be copied. Please check your input parameters or table content")

    println("Partitions to be copied: " + paths.mkString("\n"))
    val sourceAbsTblLoc = getTableLocation(sourceDbName, sourceTableName)
    val targetAbsTblLoc = getTableLocation(targetDbName, targetTableName)
    val fs = getFileSystem(confEx, sourceAbsTblLoc)

    val allSourceFiles = paths.map(x => listRecursively(fs, new Path(x)).filter(!_.isDirectory)).reduce(_ union _)
    println("Total number of files to be copied: " + allSourceFiles.length)
    val sourceTargetPaths = allSourceFiles.map(x => Paths(x.path, x.path.replace(sourceAbsTblLoc, targetAbsTblLoc)))
    println("Partitions of table " + sourceDbName + "." + sourceTableName + " which are going to be copied to " + targetDbName + "." + targetTableName + ":")
    sourceTargetPaths.slice(0, 10).foreach(x => println(x))
    val res = DistributedExecution.copyFiles(sourceAbsTblLoc, targetAbsTblLoc, sourceTargetPaths, partitionCount)
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
  @deprecated
  def moveTablePartitions(sourceDbName: String, sourceTableName: String, targetDbName: String, targetTableName: String,
                          matchStringPartitions: Seq[String] = Seq(), moveContentOnly: Boolean = false,
                          numOfThreads: Int = 1000, timeoutMin: Int = 10)
                         (implicit spark: SparkSession, conf: Configuration): Array[FSOperationResult] = {
    val partitionFoldersUriPaths = filterPartitions(sourceDbName, sourceTableName, matchStringPartitions)
    val sourceAbsTblLoc = getTableLocation(sourceDbName, sourceTableName)
    val targetAbsTblLoc = getTableLocation(targetDbName, targetTableName)
    val sourceTargetPaths = partitionFoldersUriPaths.map(x => Paths(x, x.replace(sourceAbsTblLoc, targetAbsTblLoc)))
    println("Partitions of table " + sourceDbName + "." + sourceTableName + " which are going to be moved to " + targetDbName + "." + targetTableName + ":")
    //todo add check for empty list
    sourceTargetPaths.foreach(x => println(x))
    val fs = getFileSystem(conf, sourceAbsTblLoc)
    val allSourceFiles = partitionFoldersUriPaths.
      map(x => listRecursively(fs, new Path(x)).filter(!_.isDirectory).map(_.toRelativePath()).map(_.path))
      .reduce(_ union _)
    val targetFiles = allSourceFiles.map(_.replace(getRelativePath(sourceAbsTblLoc), getRelativePath(targetAbsTblLoc)))
    val paths = allSourceFiles.zip(targetFiles).map(x => Paths(x._1, x._2))
    println("Deleting targets...")
    val targetPathsForDelete = partitionFoldersUriPaths.map(_.replace(sourceAbsTblLoc, targetAbsTblLoc)).filter(x => fs.exists(new Path(x))).map(y => getRelativePath(y))
    LocalExecution.deletePaths(fs,targetPathsForDelete, timeoutMin,numOfThreads)
    println("Moving files...")
    val res = LocalExecution.moveFiles(paths, sourceAbsTblLoc, numOfThreads, timeoutMin)
    refreshMetadata(sourceDbName, sourceTableName)
    refreshMetadata(targetDbName, targetTableName)
    res
  }

  def moveTablePartitionFolders(sourceDbName: String, sourceTableName: String, targetDbName: String, targetTableName: String,
                                matchStringPartitions: Seq[String] = Seq(), moveContentOnly: Boolean = false,
                                numOfThreads: Int = 1000, timeoutMin: Int = 10)
                               (implicit spark: SparkSession, conf: Configuration): Array[FSOperationResult] = {
    //must add preper rerun
    val partitionFoldersUriPaths = filterPartitions(sourceDbName, sourceTableName, matchStringPartitions)
    val sourceAbsTblLoc = getTableLocation(sourceDbName, sourceTableName)
    val targetAbsTblLoc = getTableLocation(targetDbName, targetTableName)
    val sourceTargetUriPaths = partitionFoldersUriPaths.map(x => Paths(x, x.replace(sourceAbsTblLoc, targetAbsTblLoc)))
    val fs = getFileSystem(conf, sourceAbsTblLoc)
    checkIfFsIsTheSame(sourceAbsTblLoc, targetAbsTblLoc) //throws exception in case of discrepancy

    println("Partitions of table " + sourceDbName + "." + sourceTableName + " which are going to be moved to " + targetDbName + "." + targetTableName + ":")
    //todo add check for empty list
    if (partitionFoldersUriPaths.isEmpty)
      sourceTargetUriPaths.foreach(x => println(x))

    //check if it is safe to move folders
    sourceTargetUriPaths.foreach(x =>
      if (!doesMoveLookSafe(fs, getRelativePath(x.sourcePath), getRelativePath(x.targetPath))) {
        throw new Exception("It looks like there is a path in source " + x.sourcePath + " which is empty, but target folder " + x.targetPath + " is not. " +
          "Promotor tries to protect you from unintentional deletion of your data in the target. If you want to avoid this exception, " +
          "place at least one empty file in the source folder to avoid run interruption")
      }
    )

    val existingTargetFolders = partitionFoldersUriPaths.map(_.replace(getRelativePath(sourceAbsTblLoc), getRelativePath(targetAbsTblLoc))).
      filter(folder => fs.exists(new Path(folder)))
    //make backup
    val backups = existingTargetFolders.map(x => x + "_promotor_backup")
    val backupPaths = existingTargetFolders.zip(backups).map(x => Paths(x._1, x._2))
    println("Moving target folders before deletion...\n" + backupPaths.slice(0, 5).mkString("\n"))
    LocalExecution.moveFiles(backupPaths, sourceAbsTblLoc, numOfThreads, timeoutMin)

    println("Backups done.. Now moving source ")
    val paths = sourceTargetUriPaths.map(x => Paths(getRelativePath(x.sourcePath), getRelativePath(x.targetPath)))
    val res = LocalExecution.moveFiles(paths, sourceAbsTblLoc, numOfThreads, timeoutMin)
    refreshMetadata(sourceDbName, sourceTableName)
    refreshMetadata(targetDbName, targetTableName)

    println("Move is done. Now deleting the backups...")
    LocalExecution.deletePaths(fs, backups)
    println("You're all set!..")

    res
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
