import TestPartitionMove.spark
import com.pg.bigdata.utils.Assistant
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit

object TestUtils {

  def setupTestEnv()(implicit spark: SparkSession)={
    spark.sparkContext.setLogLevel("ERROR")
    com.pg.bigdata.utils.fs.magicPrefix = "file:"
    spark.sql("create database promotor")
    spark.catalog.setCurrentDatabase("promotor")

    spark.read.parquet("data/sfct").where("mm_time_perd_end_date != '2019-12-31'").
      write.partitionBy("mm_time_perd_end_date").
      option("path","data/testfield/STORE_SALES_FCT").saveAsTable("STORE_SALES_FCT")
    println("Creation of STORE_SALES_FCT - done")
    spark.read.parquet("data/sfct").where("mm_time_perd_end_date != '2019-12-31'").
      write.partitionBy("mm_time_perd_end_date").
      option("path","data/testfield/STORE_SALES_SFCT").saveAsTable("STORE_SALES_SFCT")
    println("Creation of STORE_SALES_SFCT - done")
    spark.read.parquet("data/sfct").where("mm_time_perd_end_date > '2019-07-31'").
      write.partitionBy("mm_time_perd_end_date").
      option("path","data/testfield/STORE_SALES_DLT").saveAsTable("STORE_SALES_DLT")
    println("Creation of STORE_SALES_DLT - done")
    spark.read.parquet("data/sfct").where("mm_time_perd_end_date < '2019-10-31'").
      write.partitionBy("mm_time_perd_end_date").
      option("path","data/testfield/STORE_SALES_PREV").saveAsTable("STORE_SALES_FCT_PREV")
    println("Creation of STORE_SALES_PREV - done")
    spark.read.parquet("data/prod_dim").write.option("path","data/testfield/PROD_DIM").saveAsTable("PROD_DIM")
    println("Creation of PROD_DIM - done")
    spark.read.parquet("data/prod_dim").withColumn("pg_categ_txt",lit("updated")).limit(200).
      write.option("path","data/testfield/PROD_SDIM").saveAsTable("PROD_SDIM")
    println("Creation of PROD_SDIM - done")

    println("***********************************")
    println("*** HIVE TABLES SETUP COMPLETE   **")
    println("***********************************")

//modifying content of a partition of DLT to confirm successful move/copy
    assert(spark.table("STORE_SALES_SFCT").filter("mm_time_perd_end_date = '2019-10-31'").select("prod_id").distinct.count > 1,
      "check to make sure there are multiple prod IDs before promotion/modification")

    //modify partition content to make sure later, that partition was actually exchanged
    val modifiedDLT = spark.table("STORE_SALES_DLT").filter("mm_time_perd_end_date = '2019-10-31'").
      withColumn("prod_id",lit("exchange"))
    modifiedDLT.write.option("path","data/testfield/TMP").saveAsTable("TMP")
    modifiedDLT.show()
    spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
    spark.table("TMP").write.mode("overwrite").insertInto("STORE_SALES_DLT")

    val distinctProdIdCntInit = spark.table("STORE_SALES_DLT").filter("mm_time_perd_end_date = '2019-10-31'").select("prod_id").distinct.count
    assert( distinctProdIdCntInit == 1,
      "check if partition was modified correctly - actual count of distinct prod ids: "+distinctProdIdCntInit)
    //#test1
    val currPrttnCnt = spark.table("STORE_SALES_DLT").
      filter("mm_time_perd_end_date in ('2019-12-31','2019-10-31')").select("mm_time_perd_end_date").distinct.count()
    assert(currPrttnCnt == 2,
      "check if partition to be promoted exists in source - actual: "+currPrttnCnt)
  }

}
