import com.pg.bigdata.utils.Promotor
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FlatSpec}

class TestCopyOverwriteNonpartitionedTable extends FlatSpec with BeforeAndAfterAll {

  implicit val spark: SparkSession = SparkSession.builder().
    appName("NAS_").
    master("local[1]").
    getOrCreate()
  spark.conf.set("spark.speculation", "false")
  implicit val c = spark.sparkContext.hadoopConfiguration
  val t = new TestUtils("abc")
  t.setupTestEnv()


  println("Checking initial tables")
  val pdcIni = spark.table(t.d+"PROD_DIM").select("pg_categ_txt").distinct
  val pscIni = spark.table(t.d+"PROD_SDIM").select("pg_categ_txt").distinct
  println("PROD_DIM initial categ distinct")
  pdcIni.show()
  println("PROD_SDIM initial categ distinct")
  pscIni.show()
  assert(pdcIni.count() > 1 && pscIni.count() == 1, "Initial versions of prod (s)dim check failed")

  Promotor.copyOverwriteTable(t.db, "PROD_SDIM", t.db, "PROD_DIM", 1)


  "PROD_SDIM" should "not be modified after copy a it is the source" in {
    val sdim = spark.table(t.d+"PROD_SDIM")
    sdim.show()
    println("Checking count of rows in PROD_SDIM after promotion (200 expected)")
    assert(sdim.count() == 200, "Count of rows is not 200 - actual " + sdim.count())
  }

   "PROD_DIM" should "contain 200 rows as the source table" in {
    println("Checking count of rows in PROD_DIM after promotion (200 expected)")
    val pd = spark.table(t.d+"PROD_DIM")
    pd.show()
    assert(pd.count() == 200, "Count of rows is not 200 - actual " + pd.count())
  }

  "PROD_DIM" should "contain data from SDIM (pg categ value should have string 'updated'" in {
    println("Checking if pg_categ_txt in PROD_DIM has only one value \"updated\"")
    val dc = spark.table(t.d+"PROD_DIM").select("pg_categ_txt").distinct
    assert(dc.count() == 1, "Count distinct categs is not 1 - actual " + dc.count())
    assert(dc.collect().map(_.getAs[String]("pg_categ_txt")).head == "updated")
  }

  override def afterAll() =  t.cleanup()


}
