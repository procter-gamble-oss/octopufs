import com.pg.bigdata.octopufs.Promotor
import org.apache.spark.sql.functions._
import org.scalatest.flatspec.AnyFlatSpec
import testsupport.{AssertionsHelper, HadoopSimpleAuth, LocalSparkSuite}

class TestPartitionCopy extends AnyFlatSpec with LocalSparkSuite with HadoopSimpleAuth with AssertionsHelper {
  private val db = "promotor_TestPartitionCopy"
  private val d = db + "."

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    spark.sql(s"create database if not exists $db")

    // Seed DLT with partitions including 2019-12-31 and 2019-10-31
    val dlt = spark.range(0, 100)
      .withColumn("mm_time_perd_end_date", expr("CASE WHEN id % 2 = 0 THEN '2019-12-31' ELSE '2019-10-31' END"))
      .withColumn("prod_id", lit("prod"))
    dlt.write.partitionBy("mm_time_perd_end_date").mode("overwrite").saveAsTable(d + "STORE_SALES_DLT")

    // Seed SFCT without 2019-12-31 partition
    val sfct = spark.range(0, 100)
      .withColumn("mm_time_perd_end_date", expr("CASE WHEN id % 5 = 0 THEN '2019-10-31' ELSE '2019-08-31' END"))
      .withColumn("prod_id", lit("prod"))
    sfct.write.partitionBy("mm_time_perd_end_date").mode("overwrite").saveAsTable(d + "STORE_SALES_SFCT")
  }

  "After partition copy, STORE_SALES_SFCT" should "contain new partition and retain others" in {
    implicit val conf = spark.sparkContext.hadoopConfiguration
    implicit val sparkSession = spark
    Promotor.copyTablePartitions(db, "STORE_SALES_DLT", db, "STORE_SALES_SFCT", Seq("2019-12-31"), 1)
    val target = spark.table(d + "STORE_SALES_SFCT").filter("mm_time_perd_end_date = '2019-12-31'")
    val source = spark.table(d + "STORE_SALES_DLT").filter("mm_time_perd_end_date = '2019-12-31'")
    assert(target.count() == source.count())
    assert(target.select("prod_id").distinct.count() == source.select("prod_id").distinct.count())
  }

  override protected def afterAll(): Unit = {
    try {
      spark.sql(s"drop table if exists $d" + "STORE_SALES_DLT")
      spark.sql(s"drop table if exists $d" + "STORE_SALES_SFCT")
      spark.sql(s"drop database if exists $db")
    } finally super.afterAll()
  }
}
