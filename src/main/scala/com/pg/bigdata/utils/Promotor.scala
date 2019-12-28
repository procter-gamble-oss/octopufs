package com.pg.bigdata.utils

import com.pg.bigdata.utils.ConfigSerDeser
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.TableIdentifier
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
//val magicPrefix = ".dfs.core.windows.net"

//gets a list of source and target relative paths (relative to base file system)

case class Paths(sourcePath: String, targetPath: String)

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils

object Promotor extends Serializable {

  val magicPrefix = ".dfs.core.windows.net"

  def getTableLocation(tableName: String)(implicit spark: SparkSession): String = {
    getTableLocation(spark.catalog.currentDatabase,tableName)
  }

  def copyFilesBetweenTables(sourceTableName: String, targetTableName: String, partitionCnt: Int)
                            (implicit spark: SparkSession, confEx: Configuration): Boolean = {
    val db = spark.catalog.currentDatabase
    copyFilesBetweenTables(db, sourceTableName, db, targetTableName, partitionCnt)
  }

  def copyFilesBetweenTables(sourceDbName: String, sourceTableName: String,
                             targetDbName: String, targetTableName: String, partitionCount: Int = 192)
                            (implicit spark: SparkSession, confEx: Configuration): Boolean = {

    val paths = Promotor.getPathsList(sourceDbName, sourceTableName,targetDbName, targetTableName)//.take(5)
    if(paths.isEmpty)
      throw new Exception("No files to be copied")
    //todo moze moge to usunac jesli systemowy setup zostanie zrobiony przez adls utils
    //val conf = credentialsConfiguration()
    val srcLoc = Promotor.getTableLocation(sourceDbName,sourceTableName)
    val trgLoc = Promotor.getTableLocation(targetDbName,targetTableName)
    val sdConf = new ConfigSerDeser(confEx)
    val requestProcessed = spark.sparkContext.longAccumulator("FilesProcessedCount")
    print(paths(0))
    val res = spark.sparkContext.parallelize(paths,partitionCount).mapPartitions(x => {
      val conf = sdConf.get()
      val srcFs = Promotor.getFileSystem(conf, srcLoc)
      val trgFs = Promotor.getFileSystem(conf, trgLoc)
      x.map(paths => {
        requestProcessed.add(1)
        (paths, Promotor.copySingleFile(conf, paths.sourcePath,paths.targetPath,srcFs,trgFs))
      })
    }).collect()
    println("Number of files copied properly: "+ res.count(_._2))
    println("Files with errors: "+ res.count(!_._2))
    refreshMetadata(targetDbName, targetTableName)
    res.exists(!_._2)
  }

  def getContainerName(uri: String): String = {
    uri.substring(uri.indexOf("//")+2,uri.indexOf("@"))
  }

  def copySingleFile(hadoopConf: Configuration, sourcePath: String, targetPath: String,
                     sourceFileSystem: FileSystem, targetFileSystem: FileSystem,
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

  def moveFilesBetweenTables(sourceDbName: String, sourceTableName: String,
                             targetDbName: String, targetTableName: String, partitionCount: Int = 192)
                            (implicit spark: SparkSession, confEx: Configuration): Boolean = {

    val paths = Promotor.getPathsList(sourceDbName, sourceTableName,targetDbName, targetTableName)//.take(5)
    if(paths.isEmpty)
      throw new Exception("No files to be copied")
    val srcLoc = Promotor.getTableLocation(sourceDbName,sourceTableName)
    val trgLoc = Promotor.getTableLocation(targetDbName,targetTableName)

    if(getFileSystemPrefix(trgLoc)+getContainerName(trgLoc) != getFileSystemPrefix(srcLoc)+getContainerName(srcLoc))
      throw new Exception("Cannot move files between 2 different filesystems")

// moze moge to usunac jesli systemowy setup zostanie zrobiony przez adls utils
   // val conf = credentialsConfiguration()
    val sdConf = new ConfigSerDeser(confEx) //serializing bloody configuration
    val requestProcessed = spark.sparkContext.longAccumulator("FilesProcessedCount")
    println(paths(0))
    println("Files to be moved: "+paths.length)

    val res = spark.sparkContext.parallelize(paths,partitionCount).mapPartitions(x => {
      val conf = sdConf.get()
      val srcFs = Promotor.getFileSystem(conf, srcLoc) //move can be done only within single fs, which makes sense :)
      x.map(paths => Future{
        requestProcessed.add(1)
        (paths, srcFs.rename(new Path(paths.sourcePath),new Path(paths.targetPath)))
      })
    }).map(x => Await.result(x, 10 seconds)).collect
    println("Number of files moved properly: "+ res.count(_._2))
    println("Files with errors: "+ res.count(!_._2))
    refreshMetadata(targetDbName, targetTableName)
    res.exists(!_._2)
  }

  def getPathsList(sourceDbName: String, sourceTableName: String,
                   targetDbName: String, targetTableName: String)
                  (implicit spark: SparkSession): Array[Paths] = {
    val sourceLocation = Promotor.getRelativePath(Promotor.getTableLocation(sourceDbName,sourceTableName))
    val targetLocation = Promotor.getRelativePath(Promotor.getTableLocation(targetDbName,targetTableName))
    println("target location " +targetLocation)
    val sourceFileList = getListOfTableFiles(sourceDbName, sourceTableName)
    if(sourceFileList.isEmpty)
      throw new Exception("There is nothing to be copied")
    val targetFileList = sourceFileList.map(_.replaceAll(sourceLocation, targetLocation))
    println(targetFileList(0))
    sourceFileList.zip(targetFileList).map(x => Paths(x._1,x._2))
  }

  def getListOfTableFiles(sourceDbName: String, sourceTableName: String)(implicit spark: SparkSession): Array[String] = {
    spark.table(sourceDbName + "." + sourceTableName).inputFiles.map(x => Promotor.getRelativePath(x))

  }

  def refreshMetadata(db: String, table: String)(implicit spark: SparkSession): Unit = {
    val target = db + "." + table
    println("Refreshing metadata for " + target)
    spark.catalog.refreshTable(target)
    if(getTableMetadata(db, table).partitionColumnNames.nonEmpty) {
      println("Recovering partitions for "+target)
      spark.catalog.recoverPartitions(target)
    }

  }

  def deleteTablePartitions(db: String, tableName: String, matchStringPartitions: Seq[String])(implicit spark: SparkSession, conf: Configuration): Unit = {
    val paths = filterPartitions(db,tableName,matchStringPartitions)
    val absTblLoc = getTableLocation(db,tableName)
    println("Partitions of table "+db+"."+tableName+ " which are going to be deleted:")
    paths.foreach(println)
    val fs = getFileSystem(conf, absTblLoc)
    val r = paths.map(x => {
      val x = fs.delete(new Path(x),true)
      if(x) println(x + " deleted")
      else  println("Could not delete "+x)
      x
    }).filter(_ == false)
    if(r.nonEmpty) throw new Exception("Deleting of some partitions failed")

  }

  def filterPartitions(db: String, tableName: String, partitionsToKeepLike: Seq[String])(implicit spark: SparkSession): Array[String] = {
    getTableL1PartitionsPaths(db,tableName).filter(x => partitionsToKeepLike.exists(y => x.contains(y)))
  }

  def getTableL1PartitionsPaths(db: String, tableName: String)(implicit spark: SparkSession): Array[String] = {
    val m = getTableMetadata(db,tableName).partitionColumnNames
    if(m.isEmpty) throw new Exception("Table " + db + "." + tableName + " is not partitioned")
    val absTblLoc = getTableLocation(db, tableName)
    val tblLoc = getRelativePath(absTblLoc)
    val fs = getFileSystem(spark.sparkContext.hadoopConfiguration, absTblLoc)
    val partList = fs.listStatus(new Path(tblLoc))
    partList.filter(_.isDirectory).map(tblLoc + _.getPath.getName)
  }

  def getTableLocation(databaseName: String, tableName: String)(implicit spark: SparkSession): String = {
    getTableMetadata(databaseName, tableName).location.toString
  }

  def getRelativePath(uri: String): String = {
    if(!uri.contains(magicPrefix))
      throw new Exception("MagicPrefix not found")
    uri.substring(uri.indexOf(magicPrefix)+magicPrefix.length)
  }

  def getFileSystem(hadoopConf: Configuration, absoluteTargetLocation: String): FileSystem = {
    hadoopConf.set("fs.defaultFS", Promotor.getFileSystemPrefix(absoluteTargetLocation))
    FileSystem.get(hadoopConf)
  }

  def getFileSystemPrefix(uri: String): String = {
    if(!uri.contains(magicPrefix))
      throw new Exception("MagicPrefix not found")
    uri.substring(0,uri.indexOf(magicPrefix)+magicPrefix.length)
  }

  private def getTableMetadata(databaseName: String, tableName: String)(implicit spark: SparkSession): CatalogTable = {
    spark.sessionState.catalog.getTableMetadata(new TableIdentifier(tableName, Option(databaseName)))
  }


}
/*
def credentialsConfiguration()(implicit spark: SparkSession) ={
  val storageName = "adls2nas001"
  val principalID = "b74d7952-eea1-43bc-b0bb-a1088e6d32f5"
  val secretName = "propensity-storage-principal"
  val secretScopeName = "propensity"

  val conf = spark.sparkContext.hadoopConfiguration
  conf.set("fs.azure.account.auth.type." + storageName + ".dfs.core.windows.net", "OAuth")
  //conf.set("fs.azure.account.key.adls2nas001.dfs.core.windows.net","xxxxx")
  conf.set("fs.azure.account.oauth.provider.type." + storageName + ".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
  conf.set("fs.azure.account.oauth2.client.id." + storageName + ".dfs.core.windows.net", principalID)
  conf.set("fs.azure.account.oauth2.client.secret." + storageName + ".dfs.core.windows.net", dbutils.secrets.get(scope = secretScopeName, key = secretName))
  conf.set("fs.azure.account.oauth2.client.endpoint." + storageName + ".dfs.core.windows.net", "https://login.microsoftonline.com/3596192b-fdf5-4e2c-a6fa-acb706c963d8/oauth2/token")
  conf
}*/
