import com.pg.bigdata.utils.Promotor
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit

object TestPartitionCopyOverwrite extends App {
  implicit val spark: SparkSession = SparkSession.builder().
    appName("NAS_").
    master("local").
    getOrCreate()
  implicit val c = spark.sparkContext.hadoopConfiguration
  TestUtils.setupTestEnv()

  //#test1
  val currPrttnCnt = spark.table("STORE_SALES_DLT").
    filter("mm_time_perd_end_date in ('2019-12-31','2019-10-31')").select("mm_time_perd_end_date").distinct.count()
  assert(currPrttnCnt == 2,
    "check if partition to be promoted exists in source - actual: "+currPrttnCnt)

  println("Partitions in DLT")
  spark.table("STORE_SALES_DLT").select("mm_time_perd_end_date").distinct.show()

  assert(spark.table("STORE_SALES_SFCT").filter("mm_time_perd_end_date = '2019-12-31'").count() == 0,
    "check if partition to be promoted does not exists in target")
  println("Partitions in SFCT")
  spark.table("STORE_SALES_SFCT").select("mm_time_perd_end_date").distinct.show()

  Promotor.copyOverwritePartitions("STORE_SALES_DLT","STORE_SALES_SFCT",Seq("2019-10-31","2019-12-31"), 1)

  println("SFCT partitions after promotion:")
  spark.table("STORE_SALES_SFCT").select("mm_time_perd_end_date").distinct.show()
  assert(spark.table("STORE_SALES_SFCT").filter("mm_time_perd_end_date = '2019-12-31'").count()!=0, "check if partition was correctly copied")
  val distinctProdIdCnt = spark.table("STORE_SALES_SFCT").filter("mm_time_perd_end_date = '2019-10-31'").select("prod_id").distinct.count
  assert( distinctProdIdCnt == 1,
    "check if partition was fully replaced with new one - actual:"+distinctProdIdCnt)
  assert(spark.table("STORE_SALES_SFCT").select("mm_time_perd_end_date").distinct.count==5, "check if other partitions remained")

}
