package com.pg.bigdata.utils

import com.pg.bigdata.utils.Assistant._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, LocatedFileStatus, Path, RemoteIterator}
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

//val magicPrefix = ".dfs.core.windows.net"
case class Paths(sourcePath: String, targetPath: String) {
  override def toString(): String = sourcePath + " -->> " + targetPath
}

case class FSOperationResult(path: String, success: Boolean)


object Promotor extends Serializable {

  def copyFilesBetweenTables(sourceTableName: String, targetTableName: String, partitionCnt: Int)
                            (implicit spark: SparkSession, confEx: Configuration): Dataset[FSOperationResult] = {
    val db = spark.catalog.currentDatabase
    copyFilesBetweenTables(db, sourceTableName, db, targetTableName, partitionCnt)
  }

  def copyFilesBetweenTables(sourceDbName: String, sourceTableName: String, targetDbName: String, targetTableName: String, partitionCount: Int = 192)
                            (implicit spark: SparkSession, confEx: Configuration): Dataset[FSOperationResult] = {
    val paths = getPathsList(sourceDbName, sourceTableName, targetDbName, targetTableName) //.take(5)
    if (paths.isEmpty)
      throw new Exception("No files to be copied")

    val srcLoc = getTableLocation(sourceDbName, sourceTableName)
    val trgLoc = getTableLocation(targetDbName, targetTableName)
    copyFolder(srcLoc, trgLoc,partitionCount)
  }

  def copyFolder(sourceFolderUri: String, targetLocationUri: String, partitionCount: Int = 192)(implicit spark: SparkSession, confEx: Configuration): Dataset[FSOperationResult] = {
    val srcFs = getFileSystem(confEx, sourceFolderUri)
    val sourceFileList = listFilesRecursively(srcFs,sourceFolderUri)
    val rspath = getRelativePath(sourceFolderUri)
    val rtpath = getRelativePath(targetLocationUri)
    val targetFileList = sourceFileList.map(_.replaceAll(rspath, rtpath))
    println(targetFileList.head)
    val paths = sourceFileList.zip(targetFileList).map(x => Paths(x._1, x._2))

    println(paths.head)
    copyFiles(sourceFolderUri, targetLocationUri, paths, partitionCount)

  }
 //if target folder exists, it will be deleted first
  def moveFolder(sourceFolderUri: String, targetLocationUri: String, partitionCount: Int = 192)(implicit spark: SparkSession, confEx: Configuration): Dataset[FSOperationResult] = {
    if (getFileSystemPrefix(targetLocationUri) + getContainerName(targetLocationUri) != getFileSystemPrefix(sourceFolderUri) + getContainerName(sourceFolderUri))
      throw new Exception("Cannot move files between 2 different filesystems. Use copy instead")
    val srcFs = getFileSystem(confEx, sourceFolderUri)
    val trgFs = getFileSystem(confEx, targetLocationUri)
    //delete target folder
    if(trgFs.exists(new Path(targetLocationUri))) //todo test if it works
      trgFs.delete(new Path(targetLocationUri), true)
    val sourceFileList = listFilesRecursively(srcFs,sourceFolderUri)
    val rspath = getRelativePath(sourceFolderUri)
    val rtpath = getRelativePath(targetLocationUri)
    val targetFileList = sourceFileList.map(_.replaceAll(rspath, rtpath))
    println(targetFileList.head)
    val paths = sourceFileList.zip(targetFileList).map(x => Paths(x._1, x._2))

    println(paths.head)
    moveFiles(paths, sourceFolderUri, partitionCount)
  }

  def copyTablePartitions(sourceDbName: String, sourceTableName: String, targetDbName: String, targetTableName: String, matchStringPartitions: Seq[String] = Seq(),
                          partitionCount: Int = 192)
                         (implicit spark: SparkSession, confEx: Configuration): Dataset[FSOperationResult] = {
    val paths = filterPartitions(sourceDbName, sourceTableName, matchStringPartitions)
    val sourceAbsTblLoc = getTableLocation(sourceDbName, sourceTableName)
    val targetAbsTblLoc = getTableLocation(targetDbName, targetTableName)
    val sourceTargetPaths = paths.map(x => Paths(x,x.replace(sourceAbsTblLoc, targetAbsTblLoc)))
    println("Partitions of table " + sourceDbName + "." + sourceTableName + " which are going to be copied to "+targetDbName+"."+targetTableName+":")
    sourceTargetPaths.foreach(x => println(x))
    val resultDfs = sourceTargetPaths.map(p => copyFolder(p.sourcePath, p.targetPath,partitionCount))
    resultDfs.reduce(_ union _)
  }

  def moveTablePartitions(sourceDbName: String, sourceTableName: String, targetDbName: String, targetTableName: String, matchStringPartitions: Seq[String] = Seq(),
                          partitionCount: Int = 192)
                         (implicit spark: SparkSession, confEx: Configuration): Dataset[FSOperationResult] = {
    val paths = filterPartitions(sourceDbName, sourceTableName, matchStringPartitions).map(x => getRelativePath(x))
    val sourceAbsTblLoc = getTableLocation(sourceDbName, sourceTableName)
    val targetAbsTblLoc = getTableLocation(targetDbName, targetTableName)
    val sourceTargetPaths = paths.map(x => Paths(x,x.replace(sourceAbsTblLoc, targetAbsTblLoc)))
    println("Partitions of table " + sourceDbName + "." + sourceTableName + " which are going to be moved to "+targetDbName+"."+targetTableName+":")
    sourceTargetPaths.foreach(x => println(x))
    val resultDfs = sourceTargetPaths.map(p => moveFolder(p.sourcePath, p.targetPath,partitionCount))
    resultDfs.reduce(_ union _)
  }

  def moveFilesBetweenTables(sourceTableName: String, targetTableName: String, partitionCnt: Int)
                            (implicit spark: SparkSession, conf1: Configuration): Dataset[FSOperationResult] = {
    val db = spark.catalog.currentDatabase
    moveFilesBetweenTables(db, sourceTableName, db, targetTableName, partitionCnt)
  }

  def moveFilesBetweenTables(sourceDbName: String, sourceTableName: String, targetDbName: String, targetTableName: String, partitionCount: Int = 192)
                            (implicit spark: SparkSession, confEx: Configuration): Dataset[FSOperationResult] = {
    val srcLoc = getTableLocation(sourceDbName, sourceTableName)
    val paths = getPathsList(sourceDbName, sourceTableName, targetDbName, targetTableName) //.take(5)
    if (paths.isEmpty)
      throw new Exception("No files to be moved for path "+srcLoc)
    println(paths(0))
    println("Files to be moved: " + paths.length)
    moveFiles(paths,srcLoc,partitionCount)
  }

  def deleteAllChildObjects(folderAbsPath: String)(implicit spark: SparkSession, confEx: Configuration): Unit = {
    val relPath = getRelativePath(folderAbsPath)
    val fs = getFileSystem(spark.sparkContext.hadoopConfiguration, folderAbsPath)
    val objectList = fs.listStatus(new Path(relPath))
    val paths = objectList.map(relPath + _.getPath.getName)

    val r = deletePaths(paths, folderAbsPath)
    if (!r.filter(!_.success).isEmpty) throw new Exception("Deleting of some objects failed")
  }

  def deleteTablePartitions(db: String, tableName: String, matchStringPartitions: Seq[String])(implicit spark: SparkSession, confEx: Configuration): Unit = {
    val paths = filterPartitions(db, tableName, matchStringPartitions).map(x => getRelativePath(x))
    val absTblLoc = getTableLocation(db, tableName)
    println("Partitions of table " + db + "." + tableName + " which are going to be deleted:")
    paths.foreach(println)
    val r = deletePaths(paths, absTblLoc)
    if (!r.filter(!_.success).isEmpty) throw new Exception("Deleting of some partitions failed")
  }

  private def copySingleFile(hadoopConf: Configuration, sourcePath: String, targetPath: String, sourceFileSystem: FileSystem, targetFileSystem: FileSystem,
                             overwrite: Boolean = true, deleteSource: Boolean = false): Boolean = {
    println(sourcePath + " => " + targetPath)
    val srcPath = new Path(sourcePath)
    val destPath = new Path(targetPath)
    org.apache.hadoop.fs.FileUtil.copy(sourceFileSystem, srcPath, targetFileSystem, destPath, deleteSource, overwrite, hadoopConf)
  }

  private def copyFiles(sourceFolderUri: String, targetLocationUri: String, p: Seq[Paths],
                                partitionCount: Int)
                               (implicit spark: SparkSession, confEx: Configuration): Dataset[FSOperationResult] = {
    val confsd = new ConfigSerDeser(confEx)
    val requestProcessed = spark.sparkContext.longAccumulator("CopyFilesProcessedCount")
    val res = spark.sparkContext.parallelize(p, partitionCount).mapPartitions(x => {
      val conf = confsd.get()
      val srcFs = getFileSystem(conf, sourceFolderUri)
      val trgFs = getFileSystem(conf, targetLocationUri)
      x.map(paths => {
        requestProcessed.add(1)
        (paths.sourcePath, Promotor.copySingleFile(conf, paths.sourcePath, paths.targetPath, srcFs, trgFs))
      })
    })
    println("Number of files copied properly: " + res.filter(_._2).count)
    println("Files with errors: " + res.filter(!_._2).count)
    import spark.implicits._
    spark.createDataset(res).toDF("path","success").as[FSOperationResult]
  }

  private def deletePaths(relativePaths: Seq[String], absFolderUri: String, partitionCount: Int = 32)
                         (implicit spark: SparkSession, confEx: Configuration) = {
    val sdConf = new ConfigSerDeser(confEx)
    val res = spark.sparkContext.parallelize(relativePaths, partitionCount).mapPartitions(pathsPart => {
      val conf = sdConf.get()
      val fs = getFileSystem(conf, absFolderUri)
      pathsPart.map(path => Future {
        (path, fs.delete(new Path(path), true))
      }).map(x => Await.result(x, 10.seconds))
    })
    println("Number of paths deleted properly: " + res.filter(_._2).count)
    println("Files with errors: " + res.filter(!_._2).count)
    import spark.implicits._
    spark.createDataset(res).toDF("path","success").as[FSOperationResult]
  }

  //function assumes that move is being done within single FS. Higher level functions should check that

  private def moveFiles(relativePaths: Seq[Paths], sourceFolderUri: String, partitionCount: Int = 32)
                       (implicit spark: SparkSession, confEx: Configuration) = {
    val requestProcessed = spark.sparkContext.longAccumulator("MoveFilesProcessedCount")
    val sdConf = new ConfigSerDeser(confEx)
    val res = spark.sparkContext.parallelize(relativePaths, partitionCount).mapPartitions(x => {
      val conf = sdConf.get()
      val srcFs = getFileSystem(conf, sourceFolderUri) //move can be done only within single fs, which makes sense :)
      x.map(paths => Future {
        requestProcessed.add(1)
        (paths, srcFs.rename(new Path(paths.sourcePath), new Path(paths.targetPath)))
      })
    }).map(x => Await.result(x, 10.seconds))
    println("Number of files moved properly: " + res.filter(_._2).count)
    println("Files with errors: " + res.filter(!_._2).count)
    import spark.implicits._
    spark.createDataset(res).toDF("path","success").as[FSOperationResult]
  }

}

/*
*/
