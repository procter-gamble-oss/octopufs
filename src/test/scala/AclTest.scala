import org.apache.spark.sql.SparkSession

object AclTest extends App{
  import com.pg.bigdata.utils.ACLs.FSPermission
  import com.pg.bigdata.utils._
  implicit val spark: SparkSession = SparkSession.builder().
    appName("NAS_").
    master("local").
    getOrCreate()
  DataMockup.prepareData()
  val z = FSPermission("user", "rwx","ACCESS","11f1d713-3b19-49ec-bade-cca9a2e2a3ba")

  ACLs.getAclEntry(z)

  Promotor.getListOfTableFiles("dp_neighborhood_sales","store_sales_fct_jt").map(x => Promotor.getRelativePath(x))

  val newPermission = ACLs.FSPermission("user", "rwx","ACCESS","11f1d713-3b19-49ec-bade-cca9a2e2a3ba")
  val c = spark.sparkContext.hadoopConfiguration
  c.set("fs.azure.account.key.***REMOVED***.dfs.core.windows.net","***REMOVED***")
  //ACLs.modifyTableACLs(db, tableName, newPermission)(spark, c)
}
