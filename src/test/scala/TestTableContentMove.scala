import com.pg.bigdata.octopufs.Promotor
import org.apache.spark.sql.functions._
import org.scalatest.flatspec.AnyFlatSpec
import testsupport.{AssertionsHelper, HadoopSimpleAuth, LocalSparkSuite}

class TestTableContentMove extends AnyFlatSpec with LocalSparkSuite with HadoopSimpleAuth with AssertionsHelper {
  private val db = "promotor_TestTableContentMove"
  private val d = db + "."

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    spark.sql(s"create database if not exists $db")
    implicit val conf = spark.sparkContext.hadoopConfiguration
    implicit val sparkSession = spark
    import sparkSession.implicits._

    // Seed PROD_DIM with multiple categories
    val prodDim = Seq((1L, "a"), (2L, "b")).toDF("id", "pg_categ_txt")
    prodDim.write.mode("overwrite").saveAsTable(d + "PROD_DIM")

    // Seed PROD_SDIM with 200 rows and pg_categ_txt = 'updated'
    val prodSdim = spark.range(0, 200).withColumn("pg_categ_txt", lit("updated")).select(col("id"), col("pg_categ_txt"))
    prodSdim.write.mode("overwrite").saveAsTable(d + "PROD_SDIM")

    val pdcIni = spark.table(d + "PROD_DIM").select("pg_categ_txt").distinct
    val pscIni = spark.table(d + "PROD_SDIM").select("pg_categ_txt").distinct
    assert(pdcIni.count() > 1 && pscIni.count() == 1, "Initial versions of prod (s)dim check failed")

    Promotor.moveFilesBetweenTables(db, "PROD_SDIM", db, "PROD_DIM")
  }

  "moveFilesBetweenTables" should "remove content of source table" in {
    val sdim = spark.table(d + "PROD_SDIM")
    assert(sdim.count() == 0, s"Count of rows is not 0 - actual ${sdim.count()}")
  }

  "moveFilesBetweenTables" should "move content of SDIM to DIM" in {
    val pd = spark.table(d + "PROD_DIM")
    assert(pd.count() == 200, s"Count of rows is not 200 - actual ${pd.count()}")
    val dc = spark.table(d + "PROD_DIM").select("pg_categ_txt").distinct
    assert(dc.count() == 1, s"Count distinct categs is not 1 - actual ${dc.count()}")
    assert(dc.collect().map(_.getAs[String]("pg_categ_txt")).head == "updated")
  }

  override protected def afterAll(): Unit = {
    try {
      spark.sql(s"drop table if exists $d" + "PROD_DIM")
      spark.sql(s"drop table if exists $d" + "PROD_SDIM")
      spark.sql(s"drop database if exists $db")
    } finally super.afterAll()
  }
}
