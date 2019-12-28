package com.pg.bigdata.utils

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SparkSession

class ADLSUtils(spark: SparkSession, storageName: String, principalID: String, secretScopeName: String, secretName: String) {
  spark.sparkContext.hadoopConfiguration.set("fs.azure.account.auth.type." + storageName + ".dfs.core.windows.net", "OAuth")
  spark.sparkContext.hadoopConfiguration.set("fs.azure.account.oauth.provider.type." + storageName + ".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
  spark.sparkContext.hadoopConfiguration.set("fs.azure.account.oauth2.client.id." + storageName + ".dfs.core.windows.net", principalID)
  spark.sparkContext.hadoopConfiguration.set("fs.azure.account.oauth2.client.secret." + storageName + ".dfs.core.windows.net", dbutils.secrets.get(scope = secretScopeName, key = secretName))
  spark.sparkContext.hadoopConfiguration.set("fs.azure.account.oauth2.client.endpoint." + storageName + ".dfs.core.windows.net", "https://login.microsoftonline.com/3596192b-fdf5-4e2c-a6fa-acb706c963d8/oauth2/token")

  def setDefaultFs(containerName: String): FileSystem = {
    val hadoopConf = new Configuration(spark.sparkContext.hadoopConfiguration)
    hadoopConf.set("fs.defaultFS", "abfss://" + containerName + "@" + storageName + ".dfs.core.windows.net/")
    FileSystem.get(hadoopConf)
  }
}
