 import com.pg.bigdata.octopufs.Delta
 import com.pg.bigdata.octopufs.fs._
 import com.pg.bigdata.octopufs.helpers.implicits._
 import com.pg.bigdata.octopufs.metastore._
 import org.apache.hadoop.fs.Path
 import org.apache.spark.sql.SparkSession
 import org.apache.spark.sql.functions._
 import org.scalatest.BeforeAndAfterAll
 import org.scalatest.flatspec.AnyFlatSpec
 import org.scalatest.matchers.should.Matchers
// Duplicate removed
 import testsupport._
 
 class DeltaTest extends AnyFlatSpec with Matchers with LocalSparkSuite with HadoopSimpleAuth with TempFsHelper { self: org.scalatest.Suite =>
 
   private val db = "promotor_DeltaTest"
   private val d = db + "."
 
   private lazy val s: SparkSession = spark
 
   override protected def beforeAll(): Unit = {
     super.beforeAll()
     s.sql(s"create database if not exists $db")
     import s.implicits._
 
     // Seed SFCT with sales values
     val sfct = Seq(
       (1L, 10.0), (2L, 20.0), (3L, 30.0)
     ).toDF("id", "sales_usd_amt")
     sfct.write.mode("overwrite").option("path", tempDir("delta-sfct").toString).saveAsTable(d + "STORE_SALES_SFCT")
 
     // Seed FCT with different sales values
     val fct = Seq(
       (1L, 5.0), (2L, 15.0)
     ).toDF("id", "sales_usd_amt")
     fct.write.mode("overwrite").option("path", tempDir("delta-fct").toString).saveAsTable(d + "STORE_SALES_FCT")
   }
 
   implicit def ss: SparkSession = spark
 
   private def tableSales(tbl: String): Long =
     s.table(d + tbl).agg(sum("sales_usd_amt").alias("sales")).collect().map(_.getAs[Double]("sales")).head.toLong
 
   "init sales from SFCT and FCT" should "be different (before the test)" in {
     val salesSFCT = tableSales("STORE_SALES_SFCT")
     val salesFCT = tableSales("STORE_SALES_FCT")
     assert(salesFCT != salesSFCT, s"Sales should be different in FCT and SFCT - $salesFCT vs $salesSFCT")
   }
 
   lazy val source = getTableLocation(db, "STORE_SALES_SFCT")
   lazy val target = getTableLocation(db, "STORE_SALES_FCT")
   lazy val fs = getFileSystem(s.sparkContext.hadoopConfiguration, source)
 
   "File trees" should "be different before the test" in {
     val s1 = listLevel(fs, new Path(source)).map(x => x.path.replace(source, target))
     val t = listLevel(fs, new Path(target)).map(_.path)
     assert(s1.diff(t).nonEmpty || t.diff(s1).nonEmpty)
   }
 
   "File trees" should "be synchronized" in {
     val delta = new Delta(s.sparkContext.hadoopConfiguration)
     delta.synchronize(source, target)
     refreshMetadata(db, "STORE_SALES_FCT")(s)
   }
 
   "File trees" should "be the same after the test" in {
     val s1 = listLevel(fs, new Path(source)).map(x => x.path.replace(source, target))
     val t = listLevel(fs, new Path(target)).map(_.path)
     assert(s1.diff(t).isEmpty && t.diff(s1).isEmpty)
   }
 
   "After synchronization FCT sales" should "match SFCT" in {
     val salesSFCT = tableSales("STORE_SALES_SFCT")
     val afterSalesFCT = tableSales("STORE_SALES_FCT")
     assert(afterSalesFCT == salesSFCT, s"Sales should be the same after synchro - $salesSFCT vs $afterSalesFCT")
   }
 
   override protected def afterAll(): Unit = {
     try {
       s.sql(s"drop table if exists $d" + "STORE_SALES_SFCT")
       s.sql(s"drop table if exists $d" + "STORE_SALES_FCT")
       s.sql(s"drop database if exists $db")
     } finally super.afterAll()
   }
 }
 