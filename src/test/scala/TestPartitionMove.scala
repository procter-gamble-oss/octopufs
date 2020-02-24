import com.pg.bigdata.utils.Promotor
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit

object TestPartitionMove extends App {
  implicit val spark: SparkSession = SparkSession.builder().
    appName("NAS_").
    master("local[1]").
    getOrCreate()
  spark.conf.set("spark.speculation","false")
  implicit val c = spark.sparkContext.hadoopConfiguration
  TestUtils.setupTestEnv()
  com.pg.bigdata.utils.fs.magicPrefix="file:"

  println("Partitions in DLT")
  spark.table("STORE_SALES_DLT").select("mm_time_perd_end_date").distinct.show()
  val sfctInit = spark.table("STORE_SALES_SFCT").
  filter("mm_time_perd_end_date in ('2019-12-31','2019-10-31')").select("mm_time_perd_end_date").distinct
  sfctInit.show
  assert(sfctInit.count() == 1,
    "check if partitions to be promoted exists in target (only one should exist)")
  println("Partitions in SFCT")
  spark.table("STORE_SALES_SFCT").select("mm_time_perd_end_date").distinct.show()

  Promotor.moveTablePartitions("STORE_SALES_DLT","STORE_SALES_SFCT",Seq("2019-10-31","2019-12-31"), false, 1)

  println("SFCT partitions after promotion:")
  spark.table("STORE_SALES_SFCT").select("mm_time_perd_end_date").distinct.show()
  assert(spark.table("STORE_SALES_SFCT").filter("mm_time_perd_end_date = '2019-12-31'").count()!=0, "check if partition was correctly moved")
  val distinctProdIdCnt = spark.table("STORE_SALES_SFCT").filter("mm_time_perd_end_date = '2019-10-31'").select("prod_id").distinct.count
  assert( distinctProdIdCnt == 1,
    "check if partition was fully replaced with new one - actual:"+distinctProdIdCnt)
  assert(spark.table("STORE_SALES_SFCT").select("mm_time_perd_end_date").distinct.count==5, "check if other partitions remained")

  assert(spark.table("STORE_SALES_DLT").filter("mm_time_perd_end_date in ('2019-12-31',2019-10-31)").
    select("mm_time_perd_end_date").distinct.count==0, "check if partitions were removed from source")

}
