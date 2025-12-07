import com.pg.bigdata.octopufs.Promotor
import org.apache.spark.sql.functions._
import org.scalatest.flatspec.AnyFlatSpec
import testsupport.{AssertionsHelper, HadoopSimpleAuth, LocalSparkSuite}

class TestPartitionDelete extends AnyFlatSpec with LocalSparkSuite with HadoopSimpleAuth with AssertionsHelper {
  private val db = "promotor_TestPartitionDelete"
  private val d = db + "."

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    spark.sql(s"create database if not exists $db")

    // Seed STORE_SALES_FCT_PREV with two partitions including 2019-05-31
    val prev = spark.range(0, 50)
      .withColumn("mm_time_perd_end_date", expr("CASE WHEN id % 2 = 0 THEN '2019-05-31' ELSE '2019-06-30' END"))
      .withColumn("prod_id", lit("prod"))
    prev.write.partitionBy("mm_time_perd_end_date").mode("overwrite").saveAsTable(d + "STORE_SALES_FCT_PREV")

    assert(spark.table(d + "STORE_SALES_FCT_PREV").filter("mm_time_perd_end_date = '2019-05-31'").count() != 0)

    Promotor.deleteTablePartitions(db, "STORE_SALES_FCT_PREV", Seq("2019-05-31"))(spark)
  }

  "Partition 2019-05-31" should "be dropped" in {
    assert(spark.table(d + "STORE_SALES_FCT_PREV").filter("mm_time_perd_end_date = '2019-05-31'").count() == 0)
  }

  "Other partitions should be retained" should "keep remaining" in {
    assert(spark.table(d + "STORE_SALES_FCT_PREV").select("mm_time_perd_end_date").distinct.count == 1)
  }

  override protected def afterAll(): Unit = {
    try {
      spark.sql(s"drop table if exists $d" + "STORE_SALES_FCT_PREV")
      spark.sql(s"drop database if exists $db")
    } finally super.afterAll()
  }
}
