import com.pg.bigdata.utils.Promotor
import org.apache.spark.sql.SparkSession

object TestTableContentMove extends App {
  implicit val spark: SparkSession = SparkSession.builder().
    appName("NAS_").
    master("local[1]").
    getOrCreate()
  spark.conf.set("spark.speculation", "false")
  implicit val c = spark.sparkContext.hadoopConfiguration
  TestUtils.setupTestEnv()
  com.pg.bigdata.utils.fs.magicPrefix = "file:"

  println("Checking initial tables")
  val pdcIni = spark.table("PROD_DIM").select("pg_categ_txt").distinct
  val pscIni = spark.table("PROD_SDIM").select("pg_categ_txt").distinct
  println("PROD_DIM initial categ distinct")
  pdcIni.show()
  println("PROD_SDIM initial categ distinct")
  pscIni.show()
  assert(pdcIni.count()>1 && pscIni.count()==1, "Initial versions of prod (s)dim check failed")

  Promotor.moveFilesBetweenTables("PROD_SDIM","PROD_DIM",1)


  val sdim = spark.table("PROD_SDIM")
  sdim.show()
  println("Checking count of rows in PROD_SDIM after promotion (0 expected)")
  assert(sdim.count()==0, "Count of rows is not 0 - actual "+ sdim.count())

  println("Checking count of rows in PROD_DIM after promotion (200 expected)")
  val pd = spark.table("PROD_DIM")
  pd.show()
  assert(pd.count()==200, "Count of rows is not 200 - actual "+ pd.count())
  println("Checking if pg_categ_txt in PROD_DIM has only one value \"updated\"")
  val dc = spark.table("PROD_DIM").select("pg_categ_txt").distinct
  assert(dc.count()==1, "Count distinct categs is not 1 - actual "+ dc.count())


}
