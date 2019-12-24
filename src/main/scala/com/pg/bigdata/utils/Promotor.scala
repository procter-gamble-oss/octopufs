case class Paths(sourcePath: String, targetPath: String)

import com.pg.bigdata.utils.ConfigSerDeser
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.CatalogTable
//val magicPrefix = ".dfs.core.windows.net"

//gets a list of source and target relative paths (relative to base file system)



import com.databricks.dbutils_v1.DBUtilsHolder.dbutils

object Promotor extends Serializable {

  val magicPrefix = ".dfs.core.windows.net"

  def getTableLocation(tableName: String)(implicit spark: SparkSession): String = {
    getTableLocation(spark.catalog.currentDatabase,tableName)
  }

  def getContainerName(uri: String): String = {
    uri.substring(uri.indexOf("//")+2,uri.indexOf("@"))
  }

  def copyFilesBetweenTables(sourceTableName: String, targetTableName: String, partitionCnt: Int)(implicit spark: SparkSession): Boolean = {
    val db = spark.catalog.currentDatabase
    copyFilesBetweenTables(db, sourceTableName, db, targetTableName, partitionCnt)
  }

  def copyFilesBetweenTables(sourceDbName: String, sourceTableName: String,
                             targetDbName: String, targetTableName: String, partitionCount: Int = 192)(implicit spark: SparkSession): Boolean = {

    val paths = Promotor.getPathsList(sourceDbName, sourceTableName,targetDbName, targetTableName)//.take(5)
    if(paths.isEmpty)
      throw new Exception("No files to be copied")
    val conf = credentialsConfiguration()
    val srcLoc = Promotor.getTableLocation(sourceDbName,sourceTableName)
    val trgLoc = Promotor.getTableLocation(targetDbName,targetTableName)
    val sdConf = new ConfigSerDeser(conf)
    val requestProcessed = spark.sparkContext.longAccumulator("FilesProcessedCount")
    print(paths(0))
    val res = spark.sparkContext.parallelize(paths,partitionCount).mapPartitions(x => {
      val conf = sdConf.get
      val srcFs = Promotor.getFileSystem(conf, srcLoc)
      val trgFs = Promotor.getFileSystem(conf, trgLoc)
      x.map(paths => {
        requestProcessed.add(1)
        (paths, Promotor.copySingleFile(conf, paths.sourcePath,paths.targetPath,srcFs,trgFs))
      })
    }).collect()
    println("Number of files copied properly: "+ res.filter(_._2).size)
    println("Files with errors: "+ res.filter(!_._2).size)
    refreshMetadata(targetDbName, targetTableName)
    res.filter(!_._2).isEmpty
  }

  def copySingleFile(hadoopConf: Configuration, sourcePath: String, targetPath: String,
                     sourceFileSystem: FileSystem, targetFileSystem: FileSystem,
                     overwrite: Boolean = true, deleteSource: Boolean = false) = {
    println(sourcePath + " => " + targetPath)
    val srcPath = new Path(sourcePath)
    val destPath = new Path(targetPath)
    org.apache.hadoop.fs.FileUtil.copy(sourceFileSystem, srcPath, targetFileSystem, destPath, deleteSource, overwrite, hadoopConf)
  }

  def moveFilesBetweenTables(sourceTableName: String, targetTableName: String, partitionCnt: Int)(implicit spark: SparkSession): Boolean = {
    val db = spark.catalog.currentDatabase
    moveFilesBetweenTables(db, sourceTableName, db, targetTableName, partitionCnt)
  }

  def moveFilesBetweenTables(sourceDbName: String, sourceTableName: String,
                             targetDbName: String, targetTableName: String, partitionCount: Int = 192)(implicit spark: SparkSession): Boolean = {

    val paths = Promotor.getPathsList(sourceDbName, sourceTableName,targetDbName, targetTableName)//.take(5)
    if(paths.isEmpty)
      throw new Exception("No files to be copied")
    val srcLoc = Promotor.getTableLocation(sourceDbName,sourceTableName)
    val conf = credentialsConfiguration()
    val sdConf = new ConfigSerDeser(conf) //serializing bloody configuration
    val requestProcessed = spark.sparkContext.longAccumulator("FilesProcessedCount")
    println(paths(0))
    println("Files to be moved: "+paths.size)

    val res = spark.sparkContext.parallelize(paths,partitionCount).mapPartitions(x => {
      val conf = sdConf.get
      val srcFs = Promotor.getFileSystem(conf, srcLoc) //move can be done only within single fs, which makes sense :)
      x.map(paths => {
        requestProcessed.add(1)
        (paths, srcFs.rename(new Path(paths.sourcePath),new Path(paths.targetPath)))
      })
    }).collect
    println("Number of files moved properly: "+ res.filter(_._2).size)
    println("Files with errors: "+ res.filter(!_._2).size)
    refreshMetadata(targetDbName, targetTableName)
    res.filter(!_._2).isEmpty
  }

  def getPathsList(sourceDbName: String, sourceTableName: String,
                   targetDbName: String, targetTableName: String)
                  (implicit spark: SparkSession) = {
    val sourceLocation = Promotor.getRelativePath(Promotor.getTableLocation(sourceDbName,sourceTableName))
    val targetLocation = Promotor.getRelativePath(Promotor.getTableLocation(targetDbName,targetTableName))
    println("target location " +targetLocation)
    val sourceFileList = getListOfTableFiles(sourceDbName, sourceTableName)
    val targetFileList = sourceFileList.map(_.replaceAll(sourceLocation, targetLocation))
    println(targetFileList(0))
    sourceFileList.zip(targetFileList).map(x => Paths(x._1,x._2))
  }

  def getListOfTableFiles(sourceDbName: String, sourceTableName: String)(implicit spark: SparkSession) = {
    spark.table(sourceDbName + "." + sourceTableName).inputFiles.map(x => Promotor.getRelativePath(x))

  }

  def getRelativePath(uri: String): String = {
    // val magicPrefix = ".dfs.core.windows.net"
    uri.substring(uri.indexOf(magicPrefix)+magicPrefix.length)
  }

  def getTableLocation(databaseName: String, tableName: String)(implicit spark: SparkSession): String = {
    getTableMetadata(databaseName, tableName).location.toString
  }

  private def getTableMetadata(databaseName: String, tableName: String)(implicit spark: SparkSession): CatalogTable = {
    import org.apache.spark.sql.catalyst.TableIdentifier
    spark.sessionState.catalog.getTableMetadata(new TableIdentifier(tableName, Option(databaseName)))
  }

  def getFileSystem(hadoopConf: Configuration, absoluteTargetLocation: String) = {
    hadoopConf.set("fs.defaultFS", Promotor.getFileSystemPrefix(absoluteTargetLocation))
    FileSystem.get(hadoopConf)
  }

  def getFileSystemPrefix(uri: String): String = {
    // val magicPrefix = ".dfs.core.windows.net"
    uri.substring(0,uri.indexOf(magicPrefix)+magicPrefix.length)
  }

  def credentialsConfiguration()(implicit spark: SparkSession) ={
    val storageName = "***REMOVED***"
    val principalID = "b74d7952-eea1-43bc-b0bb-a1088e6d32f5"
    val secretName = "propensity-storage-principal"
    val secretScopeName = "propensity"
    val containerName = "dev"
    val conf = spark.sparkContext.hadoopConfiguration
    conf.set("fs.azure.account.auth.type." + storageName + ".dfs.core.windows.net", "OAuth")
    //conf.set("fs.azure.account.key.***REMOVED***.dfs.core.windows.net","***REMOVED***")
    conf.set("fs.azure.account.oauth.provider.type." + storageName + ".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
    conf.set("fs.azure.account.oauth2.client.id." + storageName + ".dfs.core.windows.net", principalID)
    conf.set("fs.azure.account.oauth2.client.secret." + storageName + ".dfs.core.windows.net", dbutils.secrets.get(scope = secretScopeName, key = secretName))
    conf.set("fs.azure.account.oauth2.client.endpoint." + storageName + ".dfs.core.windows.net", "https://login.microsoftonline.com/3596192b-fdf5-4e2c-a6fa-acb706c963d8/oauth2/token")
    conf
  }

  def refreshMetadata(db: String, table: String)(implicit spark: SparkSession) = {
    val target = db + "." + table
    println("Refreshing metadata for " + target)
    spark.catalog.refreshTable(target)
    spark.catalog.recoverPartitions(target)
  }
}