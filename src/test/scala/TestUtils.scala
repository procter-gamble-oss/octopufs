import java.io.File

import com.pg.bigdata.utils.Assistant
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.reflect.io.Directory

class TestUtils(testName: String) {
 val db = "promotor"+testName
  val d = db+"."

  val spark: SparkSession = SparkSession.builder().
    appName("NAS_").
    master("local").
    getOrCreate()

  def setupTestEnv()()={
    spark.sparkContext.setLogLevel("ERROR")

    com.pg.bigdata.utils.fs.magicPrefix = "file:"
    spark.sql("create database "+db)


    spark.read.parquet("data/sfct").where("mm_time_perd_end_date != '2019-12-31'").
      withColumn("sales_usd_amt", col("sales_usd_amt")*2).
      write.partitionBy("mm_time_perd_end_date").
      option("path","data/testfield"+testName+"/STORE_SALES_FCT").saveAsTable(d+"STORE_SALES_FCT")
    println("Creation of STORE_SALES_FCT - done")
    spark.read.parquet("data/sfct").where("mm_time_perd_end_date != '2019-12-31'").
      write.partitionBy("mm_time_perd_end_date").
      option("path","data/testfield"+testName+"/STORE_SALES_SFCT").saveAsTable(d+"STORE_SALES_SFCT")
    println("Creation of STORE_SALES_SFCT - done")
    spark.read.parquet("data/sfct").where("mm_time_perd_end_date > '2019-07-31'").
      write.partitionBy("mm_time_perd_end_date").
      option("path","data/testfield"+testName+"/STORE_SALES_DLT").saveAsTable(d+"STORE_SALES_DLT")
    println("Creation of STORE_SALES_DLT - done")
    spark.read.parquet("data/sfct").where("mm_time_perd_end_date < '2019-10-31'").
      write.partitionBy("mm_time_perd_end_date").
      option("path","data/testfield"+testName+"/STORE_SALES_PREV").saveAsTable(d+"STORE_SALES_FCT_PREV")
    println("Creation of STORE_SALES_PREV - done")
    spark.read.parquet("data/prod_dim").write.option("path","data/testfield"+testName+"/PROD_DIM").saveAsTable(d+"PROD_DIM")
    println("Creation of PROD_DIM - done")
    spark.read.parquet("data/prod_dim").withColumn("pg_categ_txt",lit("updated")).limit(200).
      write.option("path","data/testfield"+testName+"/PROD_SDIM").saveAsTable(d+"PROD_SDIM")
    println("Creation of PROD_SDIM - done")

    println("***********************************")
    println("*** HIVE TABLES SETUP COMPLETE   **")
    println("***********************************")

//modifying content of a partition of DLT to confirm successful move/copy
    assert(spark.table(d+"STORE_SALES_SFCT").filter("mm_time_perd_end_date = '2019-10-31'").select("prod_id").distinct.count > 1,
      "check to make sure there are multiple prod IDs before promotion/modification")

    //modify partition content to make sure later, that partition was actually exchanged
    val modifiedDLT = spark.table(d+"STORE_SALES_DLT").filter("mm_time_perd_end_date = '2019-10-31'").
      withColumn("prod_id",lit("exchange"))
    modifiedDLT.write.option("path","data/testfield"+testName+"/TMP").saveAsTable(d+"TMP")
    modifiedDLT.show()
    spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
    spark.table(d+"TMP").write.mode("overwrite").insertInto(d+"STORE_SALES_DLT")

    val distinctProdIdCntInit = spark.table(d+"STORE_SALES_DLT").filter("mm_time_perd_end_date = '2019-10-31'").select("prod_id").distinct.count
    assert( distinctProdIdCntInit == 1,
      "check if partition was modified correctly - actual count of distinct prod ids: "+distinctProdIdCntInit)
    //#test1
    val currPrttnCnt = spark.table(d+"STORE_SALES_DLT").
      filter("mm_time_perd_end_date in ('2019-12-31','2019-10-31')").select("mm_time_perd_end_date").distinct.count()
    assert(currPrttnCnt == 2,
      "check if partition to be promoted exists in source - actual: "+currPrttnCnt)
  }

  def cleanup()(): Unit = {
    spark.catalog.listTables(db).collect().foreach(x => spark.sql("drop table "+d+x.name))
    spark.sql("drop database "+db)
    val directory = new Directory(new File("data/testfield"+testName))
    directory.deleteRecursively()
  }

}
