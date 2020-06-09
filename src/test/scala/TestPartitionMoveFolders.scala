import com.pg.bigdata.octopufs.Promotor
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FlatSpec}

class TestPartitionMoveFolders extends FlatSpec with BeforeAndAfterAll {
  implicit val spark: SparkSession = SparkSession.builder().
    appName("NAS_").
    master("local[1]").
    getOrCreate()
  spark.conf.set("spark.speculation", "false")
  implicit val c = spark.sparkContext.hadoopConfiguration

  val t = new TestUtils("TestPartitionMoveFolders")

  "initial checks" should "be passed" in {
    t.setupTestEnv()
    println("Partitions in DLT")
    spark.table(t.d + "STORE_SALES_DLT").select("mm_time_perd_end_date").distinct.show()
    val sfctInit = spark.table(t.d + "STORE_SALES_SFCT").
      filter("mm_time_perd_end_date in ('2019-12-31','2019-10-31')").select("mm_time_perd_end_date").distinct

    println("Partitions in SFCT")
    sfctInit.show
    assert(sfctInit.count() == 1,
      "check if partitions to be promoted exists in target (only one should exist)")
    println("Partitions in SFCT")
    spark.table(t.d + "STORE_SALES_SFCT").select("mm_time_perd_end_date").distinct.show()

    Promotor.moveTablePartitionFolders(t.db, "STORE_SALES_DLT", t.db, "STORE_SALES_SFCT", Seq("2019-10-31", "2019-12-31"), false, 1)

    println("SFCT partitions after promotion:")
    spark.table(t.d + "STORE_SALES_SFCT").select("mm_time_perd_end_date").distinct.show()
  }

  "SFCT" should "contain new partition" in {
    assert(spark.table(t.d + "STORE_SALES_SFCT").filter("mm_time_perd_end_date = '2019-12-31'").count() != 0, "check if partition was correctly moved")
  }

  "SFCT" should "contain data from DLT - only one prod_id" in {
    val distinctProdIdCnt = spark.table(t.d + "STORE_SALES_SFCT").filter("mm_time_perd_end_date = '2019-10-31'").select("prod_id").distinct.count
    assert(distinctProdIdCnt == 1, "check if partition was fully replaced with new one - actual:" + distinctProdIdCnt)
  }

  "moveTablePartitions" should "retain all other partitions in the target table" in {
    assert(spark.table(t.d + "STORE_SALES_SFCT").select("mm_time_perd_end_date").distinct.count == 5, "check if other partitions remained")
  }

  "moveTablePartitions" should "removed all data of moved partitions from source table" in {
    assert(spark.table(t.d + "STORE_SALES_DLT").filter("mm_time_perd_end_date in ('2019-12-31',2019-10-31)").
      select("mm_time_perd_end_date").distinct.count == 0, "check if partitions were removed from source")
  }

  override def afterAll() = t.cleanup()

}
