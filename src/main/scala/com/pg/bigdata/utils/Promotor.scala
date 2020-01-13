package com.pg.bigdata.utils

import com.pg.bigdata.utils.Assistant._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, LocatedFileStatus, Path, RemoteIterator}
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

//val magicPrefix = ".dfs.core.windows.net"
case class Paths(sourcePath: String, targetPath: String)

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
    val sdConf = new ConfigSerDeser(confEx)

    println(paths(0))
    val res = spark.sparkContext.parallelize(paths, partitionCount).mapPartitions(x => {
      val conf = sdConf.get()
      val srcFs = getFileSystem(conf, srcLoc)
      val trgFs = getFileSystem(conf, trgLoc)
      x.map(paths => {
        (paths.sourcePath, Promotor.copySingleFile(conf, paths.sourcePath, paths.targetPath, srcFs, trgFs))
      })
    })
    println("Number of files copied properly: " + res.filter(_._2).count)
    println("Files with errors: " + res.filter(!_._2).count)
    refreshMetadata(targetDbName, targetTableName)
    import spark.implicits._
    spark.createDataset(res).as[FSOperationResult]
  }

  def copyFolders(sourceFolderUri: String, targetLocationUri: String, partitionCount: Int = 192)(implicit spark: SparkSession, confEx: Configuration) = {

    val sdConf = new ConfigSerDeser(confEx)
    val requestProcessed = spark.sparkContext.longAccumulator("FilesProcessedCount")
    val srcFs = getFileSystem(confEx, sourceFolderUri)
    val files = srcFs.listFiles(new Path(sourceFolderUri), true)

    def buildList(f: RemoteIterator[LocatedFileStatus], l: List[String]): List[String] = {
      if (files.hasNext) buildList(files, files.next().getPath().toString :: l)
      else l
    }

    val sourceFileList = buildList(files, List()).map(getRelativePath(_))
    val rspath = getRelativePath(sourceFolderUri)
    val rtpath = getRelativePath(targetLocationUri)
    val targetFileList = sourceFileList.map(_.replaceAll(rspath, rtpath))
    println(targetFileList(0))
    val paths = sourceFileList.zip(targetFileList).map(x => Paths(x._1, x._2))

    println(paths(0))
    copyMultipleFiles(sourceFolderUri, targetLocationUri, paths, sdConf, partitionCount)

  }

  private def copyMultipleFiles(sourceFolderUri: String, targetLocationUri: String, p: Seq[Paths], confsd: ConfigSerDeser,
                                partitionCount: Int)
                               (implicit spark: SparkSession, confEx: Configuration): Dataset[FSOperationResult] = {
    val requestProcessed = spark.sparkContext.longAccumulator("FilesProcessedCount")
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

  def copySingleFile(hadoopConf: Configuration, sourcePath: String, targetPath: String, sourceFileSystem: FileSystem, targetFileSystem: FileSystem,
                     overwrite: Boolean = true, deleteSource: Boolean = false): Boolean = {
    println(sourcePath + " => " + targetPath)
    val srcPath = new Path(sourcePath)
    val destPath = new Path(targetPath)
    org.apache.hadoop.fs.FileUtil.copy(sourceFileSystem, srcPath, targetFileSystem, destPath, deleteSource, overwrite, hadoopConf)
  }

  def moveFilesBetweenTables(sourceTableName: String, targetTableName: String, partitionCnt: Int)
                            (implicit spark: SparkSession, conf1: Configuration): Boolean = {
    val db = spark.catalog.currentDatabase
    moveFilesBetweenTables(db, sourceTableName, db, targetTableName, partitionCnt)
  }

  def moveFilesBetweenTables(sourceDbName: String, sourceTableName: String, targetDbName: String, targetTableName: String, partitionCount: Int = 192)
                            (implicit spark: SparkSession, confEx: Configuration): Boolean = {

    val paths = getPathsList(sourceDbName, sourceTableName, targetDbName, targetTableName) //.take(5)
    if (paths.isEmpty)
      throw new Exception("No files to be copied")
    val srcLoc = getTableLocation(sourceDbName, sourceTableName)
    val trgLoc = getTableLocation(targetDbName, targetTableName)

    if (getFileSystemPrefix(trgLoc) + getContainerName(trgLoc) != getFileSystemPrefix(srcLoc) + getContainerName(srcLoc))
      throw new Exception("Cannot move files between 2 different filesystems")

    // moze moge to usunac jesli systemowy setup zostanie zrobiony przez adls utils
    // val conf = credentialsConfiguration()
    val sdConf = new ConfigSerDeser(confEx) //serializing bloody configuration
    val requestProcessed = spark.sparkContext.longAccumulator("FilesProcessedCount")
    println(paths(0))
    println("Files to be moved: " + paths.length)

    val res = spark.sparkContext.parallelize(paths, partitionCount).mapPartitions(x => {
      val conf = sdConf.get()
      val srcFs = getFileSystem(conf, srcLoc) //move can be done only within single fs, which makes sense :)
      x.map(paths => Future {
        requestProcessed.add(1)
        (paths, srcFs.rename(new Path(paths.sourcePath), new Path(paths.targetPath)))
      })
    }).map(x => Await.result(x, 10 seconds)).collect
    println("Number of files moved properly: " + res.count(_._2))
    println("Files with errors: " + res.count(!_._2))
    refreshMetadata(targetDbName, targetTableName)
    res.exists(!_._2)
  }

  def deleteAllChildObjects(folderAbsPath: String)(implicit spark: SparkSession, confEx: Configuration) = {
    val relPath = getRelativePath(folderAbsPath)
    val fs = getFileSystem(spark.sparkContext.hadoopConfiguration, folderAbsPath)
    val objectList = fs.listStatus(new Path(relPath))
    val paths = objectList.map(relPath + _.getPath.getName)

    val r = deletePaths(paths, folderAbsPath)
    if (!r.isEmpty()) throw new Exception("Deleting of some objects failed")
  }

  def deleteTablePartitions(db: String, tableName: String, matchStringPartitions: Seq[String])(implicit spark: SparkSession, confEx: Configuration): Unit = {
    val paths = filterPartitions(db, tableName, matchStringPartitions)
    val absTblLoc = getTableLocation(db, tableName)
    println("Partitions of table " + db + "." + tableName + " which are going to be deleted:")
    paths.foreach(println)
    val r = deletePaths(paths, absTblLoc)
    if (!r.isEmpty()) throw new Exception("Deleting of some partitions failed")
  }

  private def deletePaths(paths: Seq[String], absFolderUri: String)(implicit spark: SparkSession, confEx: Configuration) = {
    val sdConf = new ConfigSerDeser(confEx)
    spark.sparkContext.parallelize(paths).mapPartitions(pathsPart => {
      val conf = sdConf.get()
      val fs = getFileSystem(confEx, absFolderUri)
      pathsPart.map(path => {
        val x = fs.delete(new Path(path), true)
        if (x) println(path + " deleted")
        else println("Could not delete " + path)
        x
      })
    }).filter(_ == false)
  }

}

/*
*/
