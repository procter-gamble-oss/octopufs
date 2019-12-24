package com.pg.bigdata.utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession


object Promotor {
  case class Paths(sourcePath: String, targetPath: String)
  val magicPrefix = ".dfs.core.windows.net"
  private def getTableLocation(databaseName: String, tableName: String)(implicit spark: SparkSession): String = {
    import org.apache.spark.sql.catalyst.TableIdentifier
    spark.catalog.setCurrentDatabase(databaseName)
    val tblMetadata = spark.sessionState.catalog.getTableMetadata(new TableIdentifier(tableName))
    tblMetadata.location.toString
  }

  def getTableLocation(tableName: String)(implicit spark: SparkSession): String = {
    getTableLocation(spark.catalog.currentDatabase,tableName)
  }

   def getListOfTableFiles(sourceDbName: String, sourceTableName: String)(implicit spark: SparkSession) = {
    spark.table(sourceDbName + "." + sourceTableName).inputFiles.map(x => getRelativePath(x))

  }
  //gets a list of source and target relative paths (relative to base file system)
  def getPathsList(sourceDbName: String, sourceTableName: String,
                             targetDbName: String, targetTableName: String)(implicit spark: SparkSession) = {
    val sourceLocation = getTableLocation(sourceDbName,sourceTableName)
    val targetLocation = getTableLocation(targetDbName,targetTableName)
    val sourceFileList = getListOfTableFiles(sourceDbName, sourceTableName).take(5)  //todo remove take
    val targetFileList = sourceFileList.map(_.replaceAll(sourceLocation, targetLocation))
    sourceFileList.zip(targetFileList).map(x => Paths(x._1,x._2))
  }

  def getRelativePath(uri: String): String = {
    uri.substring(uri.indexOf(magicPrefix)+magicPrefix.length)
  }

  def getContainerName(uri: String): String = {
    uri.substring(uri.indexOf("//")+2,uri.indexOf("@"))
  }

  def getFileSystemPrefix(uri: String): String = {
    uri.substring(0,uri.indexOf(magicPrefix)+magicPrefix.length)
  }

  def getFileSystem(hadoopConf: Configuration, absoluteTargetLocation: String) = {
    hadoopConf.set("fs.defaultFS", getFileSystemPrefix(absoluteTargetLocation))
    FileSystem.get(hadoopConf)
  }

  def copySingleFile(hadoopConf: Configuration, sourcePath: String, targetPath: String,
                     sourceFileSystem: FileSystem, targetFileSystem: FileSystem,
                     overwrite: Boolean = true, deleteSource: Boolean = false) = {
    val srcPath = new Path(sourcePath)
    val destPath = new Path(targetPath)
    org.apache.hadoop.fs.FileUtil.copy(sourceFileSystem, srcPath, targetFileSystem, destPath, deleteSource, overwrite, hadoopConf)
  }

  def copyFilesBetweenTables(sourceDbName: String, sourceTableName: String,
    targetDbName: String, targetTableName: String, partitionCount: Int = 192)(implicit spark: SparkSession): Boolean = {
    import spark.implicits._
    val paths = getPathsList(sourceDbName, sourceTableName,targetDbName, targetTableName)
    if(paths.isEmpty)
      throw new Exception("No files to be copied")
    val srcLoc = getTableLocation(sourceDbName,sourceTableName)
    val trgLoc = getTableLocation(targetDbName,targetTableName)
    val sdConf = new ConfigSerDeser(spark.sparkContext.hadoopConfiguration)
    val res = spark.sparkContext.parallelize(paths).mapPartitions(x => {
      val conf = sdConf.get
      val srcFs = getFileSystem(conf, srcLoc)
      val trgFs = getFileSystem(conf, trgLoc)
      x.map(paths => {
        (paths, copySingleFile(conf, paths.sourcePath,paths.targetPath,srcFs,trgFs))
      })
    })
    res.cache()
    println("Number of files copied properly: "+ res.filter(_._2).count)
    println("Files with errors: "+ res.filter(!_._2).count)
    res.filter(!_._2).isEmpty()
  }


  def copyFilesBetweenTables(sourceTableName: String, targetTableName: String, partitionCnt: Int)(implicit spark: SparkSession): Boolean = {
    val db = spark.catalog.currentDatabase
    copyFilesBetweenTables(db, sourceTableName, db, targetTableName, partitionCnt)
  }
}