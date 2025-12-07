import com.pg.bigdata.octopufs.Promotor
import org.apache.spark.sql.functions._
import org.scalatest.flatspec.AnyFlatSpec
import testsupport.{AssertionsHelper, HadoopSimpleAuth, LocalSparkSuite}

class TestPartitionCopyOverwrite extends AnyFlatSpec with LocalSparkSuite with HadoopSimpleAuth with AssertionsHelper {
  private val db = "promotor_TestPartitionCopyOverwrite"
  private val d = db + "."

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    spark.sql(s"create database if not exists $db")
    implicit val conf = spark.sparkContext.hadoopConfiguration
    implicit val sparkSession = spark

    // Seed DLT with two partitions
    val dlt = spark.range(0, 100)
      .withColumn("mm_time_perd_end_date", expr("CASE WHEN id % 2 = 0 THEN '2019-12-31' ELSE '2019-10-31' END"))
      .withColumn("prod_id", lit("prod"))
    dlt.write.partitionBy("mm_time_perd_end_date").mode("overwrite").saveAsTable(d + "STORE_SALES_DLT")

    // Seed SFCT with only non-overlapping partitions
    val sfct = spark.range(0, 100)
      .withColumn("mm_time_perd_end_date", expr("CASE WHEN id % 3 = 0 THEN '2019-08-31' ELSE '2019-09-30' END"))
      .withColumn("prod_id", lit("prod"))
    sfct.write.partitionBy("mm_time_perd_end_date").mode("overwrite").saveAsTable(d + "STORE_SALES_SFCT")

    // Perform overwrite copy of two partitions
    Promotor.copyOverwritePartitions(db, "STORE_SALES_DLT", db, "STORE_SALES_SFCT", Seq("2019-10-31", "2019-12-31"), 1)
  }

  "SFCT" should "contain promoted partitions after overwrite" in {
    val sfct2019_12 = spark.table(d + "STORE_SALES_SFCT").filter("mm_time_perd_end_date = '2019-12-31'")
    val dlt2019_12 = spark.table(d + "STORE_SALES_DLT").filter("mm_time_perd_end_date = '2019-12-31'")
    assert(sfct2019_12.count() == dlt2019_12.count())

    val sfct2019_10 = spark.table(d + "STORE_SALES_SFCT").filter("mm_time_perd_end_date = '2019-10-31'")
    val dlt2019_10 = spark.table(d + "STORE_SALES_DLT").filter("mm_time_perd_end_date = '2019-10-31'")
    assert(sfct2019_10.count() == dlt2019_10.count())
  }

  override protected def afterAll(): Unit = {
    try {
      spark.sql(s"drop table if exists $d" + "STORE_SALES_DLT")
      spark.sql(s"drop table if exists $d" + "STORE_SALES_SFCT")
      spark.sql(s"drop database if exists $db")
    } finally super.afterAll()
  }
}
