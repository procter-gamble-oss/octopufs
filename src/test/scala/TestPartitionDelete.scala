import com.pg.bigdata.utils.{Assistant, Promotor}
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FlatSpec}

class TestPartitionDelete extends FlatSpec with BeforeAndAfterAll{
  implicit val spark: SparkSession = SparkSession.builder().
    appName("NAS_").
    master("local").
    getOrCreate()

  val t = new TestUtils(this.getClass.getName)
  t.setupTestEnv()

    //#test1
    assert(spark.table(t.d+"STORE_SALES_FCT_PREV").filter("mm_time_perd_end_date = '2019-05-31'").count() != 0, "check if partition to be dropped exists")
    spark.table(t.d+"STORE_SALES_FCT_PREV").select("mm_time_perd_end_date").distinct.show()
    Promotor.deleteTablePartitions(t.db, "STORE_SALES_FCT_PREV", Seq("2019-05-31"))(spark, spark.sparkContext.hadoopConfiguration)
    spark.table(t.d+"STORE_SALES_FCT_PREV").select("mm_time_perd_end_date").distinct.show()

  "Partition 2019-05" should "be dropped" in {
    assert(spark.table(t.d+"STORE_SALES_FCT_PREV").filter("mm_time_perd_end_date = '2019-05-31'").count() == 0, "check if partition was correctly dropped")
  }
  "Other partitions than 019-05" should "be retained" in {
    assert(spark.table(t.d+"STORE_SALES_FCT_PREV").select("mm_time_perd_end_date").distinct.count == 2, "check if other partitions remained")
  }

  override def afterAll() =  t.cleanup()


}
