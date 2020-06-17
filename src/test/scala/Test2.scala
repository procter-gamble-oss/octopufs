import org.apache.spark.sql.{DataFrame, SparkSession}
import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
object Test2 extends App {
  implicit val spark: SparkSession = SparkSession.builder().
    appName("NAS_").
    master("local").
    getOrCreate()
 // DataMockup.prepareData()
  spark.sparkContext.setLogLevel("INFO")
  spark.catalog.setCurrentDatabase("dp_neighborhood_sales")
  //Promotor.copyFilesBetweenTables("STORE_SALES_PXM_FCT", "store_sales_fct_JT",2)(spark)


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
  }


}

