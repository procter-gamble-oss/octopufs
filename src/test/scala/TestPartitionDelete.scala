import com.pg.bigdata.utils.{Assistant, Promotor}
import org.apache.spark.sql.SparkSession

object TestPartitionDelete extends App {
  implicit val spark: SparkSession = SparkSession.builder().
    appName("NAS_").
    master("local").
    getOrCreate()

  TestUtils.setupTestEnv()

  //#test1
  assert(spark.table("STORE_SALES_FCT_PREV").filter("mm_time_perd_end_date = '2019-05-31'").count() != 0, "check if partition to be dropped exists")
  spark.table("STORE_SALES_FCT_PREV").select("mm_time_perd_end_date").distinct.show()
  Promotor.deleteTablePartitions("promotor","STORE_SALES_FCT_PREV",Seq("2019-05-31"))(spark,spark.sparkContext.hadoopConfiguration)
  spark.table("STORE_SALES_FCT_PREV").select("mm_time_perd_end_date").distinct.show()
  assert(spark.table("STORE_SALES_FCT_PREV").filter("mm_time_perd_end_date = '2019-05-31'").count()==0, "check if partition was correctly dropped")
  assert(spark.table("STORE_SALES_FCT_PREV").select("mm_time_perd_end_date").distinct.count==2, "check if other partitions remained")


}
