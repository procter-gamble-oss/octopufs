import com.pg.bigdata.utils.Assistant
import org.apache.spark.sql.SparkSession

object TestUtils {

  def setupTestEnv()(implicit spark: SparkSession)={
    spark.sparkContext.setLogLevel("ERROR")
    Assistant.magicPrefix = "file:"
    spark.sql("create database promotor")
    spark.catalog.setCurrentDatabase("promotor")

    spark.read.parquet("data/sfct").where("mm_time_perd_end_date != '2019-12-31'").
      write.partitionBy("mm_time_perd_end_date").
      option("path","data/testfield/STORE_SALES_FCT").saveAsTable("STORE_SALES_FCT")
    spark.read.parquet("data/sfct").where("mm_time_perd_end_date != '2019-12-31'").
      write.partitionBy("mm_time_perd_end_date").
      option("path","data/testfield/STORE_SALES_SFCT").saveAsTable("STORE_SALES_SFCT")
    spark.read.parquet("data/sfct").where("mm_time_perd_end_date > '2019-07-31'").
      write.partitionBy("mm_time_perd_end_date").
      option("path","data/testfield/STORE_SALES_DLT").saveAsTable("STORE_SALES_DLT")
    spark.read.parquet("data/sfct").where("mm_time_perd_end_date < '2019-10-31'").
      write.partitionBy("mm_time_perd_end_date").
      option("path","data/testfield/STORE_SALES_PREV").saveAsTable("STORE_SALES_FCT_PREV")

    println("***********************************")
    println("*** HIVE TABLES SETUP COMPLETE   **")
    println("***********************************")
  }

}
