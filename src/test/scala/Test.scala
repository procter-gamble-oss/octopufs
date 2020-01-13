import org.apache.spark.sql.{DataFrame, SparkSession}

object Test extends App {
 implicit val spark: SparkSession = SparkSession.builder().
    appName("NAS_").
    master("local").
    getOrCreate()
  DataMockup.prepareData()
  //Promotor.copyFilesBetweenTables("STORE_SALES_PXM_FCT", "STORE_SALES_TARGET",200)(spark)

}


object DataMockup {

  def prepareData()(implicit spark: SparkSession): Unit = {
      readCSV("data/store_sales_pxm_fct.csv").coalesce(10).write.option("path","data/STORE_SALES_PXM_FCT").saveAsTable("STORE_SALES_PXM_FCT")
      readCSV("data/store_sales_pxm_fct.csv").coalesce(10).where("1=2").write.option("path","Test.scaladata/STORE_SALES_TARGET").saveAsTable("STORE_SALES_TARGET")
  }
  def readCSV(path: String)(implicit spark: SparkSession): DataFrame = {
    spark.read.format("csv").
      option("header", "true").
      option("inferSchema", "true").
      load(path)
  }

}

