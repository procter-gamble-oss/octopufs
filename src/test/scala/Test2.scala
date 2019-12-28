import org.apache.spark.sql.{DataFrame, SparkSession}

object Test2 extends App {
  implicit val spark: SparkSession = SparkSession.builder().
    appName("NAS_").
    master("local").
    getOrCreate()
 // DataMockup.prepareData()
  spark.sparkContext.setLogLevel("INFO")
  spark.catalog.setCurrentDatabase("dp_neighborhood_sales")
  //Promotor.copyFilesBetweenTables("STORE_SALES_PXM_FCT", "store_sales_fct_JT",2)(spark)

}

