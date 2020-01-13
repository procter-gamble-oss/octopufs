import org.apache.spark.sql.SparkSession

object AclTest extends App{
  import com.pg.bigdata.utils.ACLs.FSPermission
  import com.pg.bigdata.utils._
  implicit val spark: SparkSession = SparkSession.builder().
    appName("NAS_").
    master("local").
    getOrCreate()
  DataMockup.prepareData()
  import com.pg.bigdata.utils.ACLs
  val acl = ACLs.FSPermission("user", "rwx","ACCESS","11f1d713-3b19-49ec-bade-cca9a2e2a3ba")
  //implicit val s = spark
  implicit val c = spark.sparkContext.hadoopConfiguration
  c.set("fs.azure.account.key.adls2nas001.dfs.core.windows.net","YOURKEY")
  ACLs.modifyTableACLs("dp_neighborhood_sales","store_sales_fct_jt",acl, 30)
}
