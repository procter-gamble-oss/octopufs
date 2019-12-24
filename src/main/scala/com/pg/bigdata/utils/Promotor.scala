case class Paths(sourcePath: String, targetPath: String)

import com.pg.bigdata.utils.ConfigSerDeser
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import com.databricks.dbutils_v1.DBUtilsHolder.dbutils

object Promotor extends Serializable {
  val magicPrefix = ".dfs.core.windows.net"

  def getListOfTableFiles(sourceDbName: String, sourceTableName: String)(implicit spark: SparkSession) = {
    spark.table(sourceDbName + "." + sourceTableName).inputFiles.map(x => Promotor.getRelativePath(x))

  }


  def getPathsList(sourceDbName: String, sourceTableName: String,
                   targetDbName: String, targetTableName: String)(implicit spark: SparkSession) = {
    val sourceLocation = Promotor.getRelativePath(Promotor.getTableLocation(sourceDbName,sourceTableName))
    val targetLocation = Promotor.getRelativePath(Promotor.getTableLocation(targetDbName,targetTableName))
    println("target location " +targetLocation)
    val sourceFileList = getListOfTableFiles(sourceDbName, sourceTableName)
    val targetFileList = sourceFileList.map(_.replaceAll(sourceLocation, targetLocation))
    println(targetFileList(0))
    sourceFileList.zip(targetFileList).map(x => Paths(x._1,x._2))
  }

  def getTableLocation(databaseName: String, tableName: String)(implicit spark: SparkSession): String = {
    import org.apache.spark.sql.catalyst.TableIdentifier
    spark.catalog.setCurrentDatabase(databaseName)
    val tblMetadata = spark.sessionState.catalog.getTableMetadata(new TableIdentifier(tableName))
    tblMetadata.location.toString
  }

  def getTableLocation(tableName: String)(implicit spark: SparkSession): String = {
    getTableLocation(spark.catalog.currentDatabase,tableName)
  }

  def copySingleFile(hadoopConf: Configuration, sourcePath: String, targetPath: String,
                     sourceFileSystem: FileSystem, targetFileSystem: FileSystem,
                     overwrite: Boolean = true, deleteSource: Boolean = false) = {
    println(sourcePath + " => " + targetPath)
    val srcPath = new Path(sourcePath)
    val destPath = new Path(targetPath)
    org.apache.hadoop.fs.FileUtil.copy(sourceFileSystem, srcPath, targetFileSystem, destPath, deleteSource, overwrite, hadoopConf)
  }

  def getFileSystemPrefix(uri: String): String = {
    // val magicPrefix = ".dfs.core.windows.net"
    uri.substring(0,uri.indexOf(magicPrefix)+magicPrefix.length)
  }




  def getRelativePath(uri: String): String = {
    // val magicPrefix = ".dfs.core.windows.net"
    uri.substring(uri.indexOf(magicPrefix)+magicPrefix.length)
  }

  def getContainerName(uri: String): String = {
    uri.substring(uri.indexOf("//")+2,uri.indexOf("@"))
  }

  def getFileSystem(hadoopConf: Configuration, absoluteTargetLocation: String) = {
    hadoopConf.set("fs.defaultFS", Promotor.getFileSystemPrefix(absoluteTargetLocation))
    FileSystem.get(hadoopConf)
  }

  def copyFilesBetweenTables(sourceDbName: String, sourceTableName: String,
                             targetDbName: String, targetTableName: String, partitionCount: Int = 192)(implicit spark: SparkSession): Boolean = {
    //import spark.implicits._
    val paths = Promotor.getPathsList(sourceDbName, sourceTableName,targetDbName, targetTableName).take(5)
    if(paths.isEmpty)
      throw new Exception("No files to be copied")
    val srcLoc = Promotor.getTableLocation(sourceDbName,sourceTableName)
    val trgLoc = Promotor.getTableLocation(targetDbName,targetTableName)
    val storageName = "adls2nas001"
    val principalID = "b74d7952-eea1-43bc-b0bb-a1088e6d32f5"
    val secretName = "propensity-storage-principal"
    val secretScopeName = "propensity"
    val containerName = "dev"
    val conf = spark.sparkContext.hadoopConfiguration
    conf.set("fs.azure.account.auth.type." + storageName + ".dfs.core.windows.net", "OAuth")
    //conf.set("fs.azure.account.key.adls2nas001.dfs.core.windows.net","***REMOVED***") 
    conf.set("fs.azure.account.oauth.provider.type." + storageName + ".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
    conf.set("fs.azure.account.oauth2.client.id." + storageName + ".dfs.core.windows.net", principalID)
    conf.set("fs.azure.account.oauth2.client.secret." + storageName + ".dfs.core.windows.net", dbutils.secrets.get(scope = secretScopeName, key = secretName))
    conf.set("fs.azure.account.oauth2.client.endpoint." + storageName + ".dfs.core.windows.net", "https://login.microsoftonline.com/3596192b-fdf5-4e2c-a6fa-acb706c963d8/oauth2/token")

    val sdConf = new ConfigSerDeser(conf)
    print(paths(0))
    val res = spark.sparkContext.parallelize(paths).mapPartitions(x => {
      val conf = sdConf.get
      //conf.set("fs.azure.account.key.adls2nasnonprod.dfs.core.windows.net", "***REMOVED***")
      val srcFs = Promotor.getFileSystem(conf, srcLoc)
      val trgFs = Promotor.getFileSystem(conf, trgLoc)
      x.map(paths => {
        (paths, Promotor.copySingleFile(conf, paths.sourcePath,paths.targetPath,srcFs,trgFs))
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
