import com.pg.bigdata.utils.Promotor
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit
import org.scalatest.{BeforeAndAfterAll, FlatSpec}

class TestPartitionCopyOverwrite extends FlatSpec with BeforeAndAfterAll{
  implicit val spark: SparkSession = SparkSession.builder().
    appName("NAS_").
    master("local").
    getOrCreate()
  implicit val c = spark.sparkContext.hadoopConfiguration
  val t = new TestUtils("TestPartitionCopyOverwrite")
  t.setupTestEnv()

  //#test1
  "initial tests" should "be passed" in {
    val currPrttnCnt = spark.table(t.d + "STORE_SALES_DLT").
      filter("mm_time_perd_end_date in ('2019-12-31','2019-10-31')").select("mm_time_perd_end_date").distinct.count()
    assert(currPrttnCnt == 2,
      "check if partition to be promoted exists in source - actual: " + currPrttnCnt)

    println("Partitions in DLT")
    spark.table(t.d + "STORE_SALES_DLT").select("mm_time_perd_end_date").distinct.show()

    assert(spark.table(t.d + "STORE_SALES_SFCT").filter("mm_time_perd_end_date = '2019-12-31'").count() == 0,
      "check if partition to be promoted does not exists in target")
    println("Partitions in SFCT")
    spark.table(t.d + "STORE_SALES_SFCT").select("mm_time_perd_end_date").distinct.show()

    Promotor.copyOverwritePartitions(t.db, "STORE_SALES_DLT", t.db, "STORE_SALES_SFCT", Seq("2019-10-31", "2019-12-31"), 1)
  }

  "SFCT" should "contain new partition after copy operation" in {
    println("SFCT partitions after promotion:")
    spark.table(t.d+"STORE_SALES_SFCT").select("mm_time_perd_end_date").distinct.show()
    assert(spark.table(t.d+"STORE_SALES_SFCT").filter("mm_time_perd_end_date = '2019-12-31'").count() != 0, "check if partition was correctly copied")
  }

  "SFCT" should "contain new/modified data from DLT table - there should be only one prod ID in partition 2019-10-31" in {
  println("Checking correctness of data promotion")
    val distinctProdIdCnt = spark.table(t.d+"STORE_SALES_SFCT").filter("mm_time_perd_end_date = '2019-10-31'").select("prod_id").distinct.count
    assert(distinctProdIdCnt == 1,"check if partition was fully replaced with new one - actual:" + distinctProdIdCnt)
    assert(spark.table(t.d+"STORE_SALES_SFCT").select("mm_time_perd_end_date").distinct.count == 5, "check if other partitions remained")
  }
  println("All ok")

   override def afterAll() =  t.cleanup()

}
