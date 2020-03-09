import java.util.concurrent.Executors

import org.apache.spark.sql.SparkSession
import com.pg.bigdata.utils.fs._

import scala.concurrent.ExecutionContext
object AclTest extends App{
  import com.pg.bigdata.utils.acl.AclManager.FsPermission
  import com.pg.bigdata.utils._
  import com.pg.bigdata.utils.acl.AclManager
  implicit val spark: SparkSession = SparkSession.builder().
    appName("NAS_").
    master("local").
    getOrCreate()
  DataMockup.prepareData()
  val acl = AclManager.FsPermission("user", "r-x","ACCESS","11f1d713-3b19-49ec-bade-cca9a2e2a3ba")
  //implicit val s = spark
  implicit val c = spark.sparkContext.hadoopConfiguration
  //c.set("fs.azure.account.key.***REMOVED***.dfs.core.windows.net","YOURKEY")
  magicPrefix="hdfs://localhost:8020"
  val fs = getFileSystem(c,"hdfs://localhost:8020/test1")
  implicit val pool = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(100))
  synchronizeAcls(fs,"/test1","/test2")
  //AclManager.modifyTableACLs("dp_neighborhood_sales","store_sales_fct_jt",acl, 30)
}
