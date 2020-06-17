import com.pg.bigdata.octopufs.Promotor
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit
import org.scalatest.{BeforeAndAfterAll, FlatSpec}

class TestPartitionCopy extends FlatSpec with BeforeAndAfterAll {
  implicit val spark: SparkSession = SparkSession.builder().
    appName("NAS_").
    master("local").
    getOrCreate()
  implicit val c = spark.sparkContext.hadoopConfiguration

  val t = new TestUtils("TestPartitionCopy")
  t.setupTestEnv()
    //modify partition content to make sure later, that partition was actually exchanged
    val modifiedDLT = spark.table(t.d+"STORE_SALES_DLT").filter("mm_time_perd_end_date = '2019-10-31'").
      withColumn("prod_id", lit("exchange"))
    modifiedDLT.localCheckpoint()
    modifiedDLT.write.insertInto(t.d+"STORE_SALES_DLT")

    assert(spark.table(t.d+"STORE_SALES_DLT").filter("mm_time_perd_end_date = '2019-12-31'").count() != 0, "check if partition to be promoted exists in source")
    println("Partitions in DLT")
    spark.table(t.d+"STORE_SALES_DLT").select("mm_time_perd_end_date").distinct.show()

    assert(spark.table(t.d+"STORE_SALES_SFCT").filter("mm_time_perd_end_date = '2019-12-31'").count() == 0, "check if partition to be promoted does not exists in target")
    println("Partitions in SFCT")
    spark.table(t.d+"STORE_SALES_SFCT").select("mm_time_perd_end_date").distinct.show()

  "After partition copy, STORE_SALES_SFCT" should "contain new partition as well as the other ones which existed there before" in {
    Promotor.copyTablePartitions(t.db, "STORE_SALES_DLT", t.db, "STORE_SALES_SFCT", Seq("2019-12-31"), 1)
    println("SFCT partitions after promotion:")
    spark.table(t.d+"STORE_SALES_SFCT").select("mm_time_perd_end_date").distinct.show()
    assert(spark.table(t.d+"STORE_SALES_SFCT").filter("mm_time_perd_end_date = '2019-12-31'").count() != 0, "check if partition was correctly copied")
    assert(spark.table(t.d+"STORE_SALES_SFCT").select("mm_time_perd_end_date").distinct.count == 5, "check if other partitions remained")
  }

  override def afterAll() =  t.cleanup()
}
