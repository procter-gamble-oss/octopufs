import com.pg.bigdata.octopufs.Promotor
import org.apache.spark.sql.functions._
import org.scalatest.flatspec.AnyFlatSpec
import testsupport.{AssertionsHelper, HadoopSimpleAuth, LocalSparkSuite}

class TestPartitionMoveFolders extends AnyFlatSpec with LocalSparkSuite with HadoopSimpleAuth with AssertionsHelper {
  private val db = "promotor_TestPartitionMoveFolders"
  private val d = db + "."

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    spark.sql(s"create database if not exists $db")
    implicit val conf = spark.sparkContext.hadoopConfiguration
    implicit val sparkSession = spark

    // Seed DLT with two partitions to be moved
    val dlt = spark.range(0, 100)
      .withColumn("mm_time_perd_end_date", expr("CASE WHEN id % 2 = 0 THEN '2019-12-31' ELSE '2019-10-31' END"))
      .withColumn("prod_id", lit("prod"))
    dlt.write.partitionBy("mm_time_perd_end_date").mode("overwrite").saveAsTable(d + "STORE_SALES_DLT")

    // Seed SFCT with only one of the target partitions so precheck is 1
    val sfct = spark.range(0, 100)
      .withColumn("mm_time_perd_end_date", expr("CASE WHEN id % 3 = 0 THEN '2019-10-31' ELSE '2019-08-31' END"))
      .withColumn("prod_id", lit("prod"))
    sfct.write.partitionBy("mm_time_perd_end_date").mode("overwrite").saveAsTable(d + "STORE_SALES_SFCT")

    Promotor.moveTablePartitions(db, "STORE_SALES_DLT", db, "STORE_SALES_SFCT", Seq("2019-10-31", "2019-12-31"))
  }

  "SFCT" should "contain new partition 2019-12-31 after move" in {
    assert(spark.table(d + "STORE_SALES_SFCT").filter("mm_time_perd_end_date = '2019-12-31'").count() != 0)
  }

  "SFCT" should "contain data from DLT - only one prod_id for 2019-10-31" in {
    val distinctProdIdCnt = spark.table(d + "STORE_SALES_SFCT").filter("mm_time_perd_end_date = '2019-10-31'").select("prod_id").distinct.count
    assert(distinctProdIdCnt == 1)
  }

  "moveTablePartitions" should "remove moved partitions from source DLT" in {
    assert(spark.table(d + "STORE_SALES_DLT").filter("mm_time_perd_end_date in ('2019-12-31','2019-10-31')").select("mm_time_perd_end_date").distinct.count == 0)
  }

  override protected def afterAll(): Unit = {
    try {
      spark.sql(s"drop table if exists $d" + "STORE_SALES_DLT")
      spark.sql(s"drop table if exists $d" + "STORE_SALES_SFCT")
      spark.sql(s"drop database if exists $db")
    } finally super.afterAll()
  }
}
