import com.pg.bigdata.octopufs.Delta
import com.pg.bigdata.octopufs.fs._
import com.pg.bigdata.octopufs.helpers.implicits._
import com.pg.bigdata.octopufs.metastore._
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.scalatest.{BeforeAndAfterAll, FlatSpec}

class DeltaTest extends FlatSpec with BeforeAndAfterAll{

  val t = new TestUtils("DeltaTest")
  t.setupTestEnv()
  val basePath = "file:/Users/jacektokar/IdeaProjects/Promoter/data"

  implicit val spark: SparkSession = t.spark

  val salesSFCT = spark.table(t.d + "STORE_SALES_SFCT").agg(sum("sales_usd_amt").
    alias("sales")).collect().map(_.getAs[Double]("sales")).head.toLong
  val salesFCT = spark.table(t.d + "STORE_SALES_FCT").agg(sum("sales_usd_amt").
    alias("sales")).collect().map(_.getAs[Double]("sales")).head.toLong

  "init sales from SFCT and FCT" should "be different (before the test)" in {
    assert(salesFCT != salesSFCT, "Sales should be different in FCT and SFCT - " + salesFCT + " vs " + salesSFCT)
  }

  val source = getTableLocation(t.db, "STORE_SALES_SFCT") //basePath+"/testfield/DeltaTest/STORE_SALES_SFCT"
  val target = getTableLocation(t.db, "STORE_SALES_FCT") //basePath+"/testfield/DeltaTest/STORE_SALES_FCT"
  val fs = getFileSystem(spark.sparkContext.hadoopConfiguration, source)

  "File trees" should "be the different before the test" in {
    val s = listLevel(fs, new Path(source)).map(x => x.path.replace(source, target))
    val t = listLevel(fs, new Path(target)).map(_.path)
    assert(s.diff(t).nonEmpty || t.diff(s).nonEmpty, "Sales before synchro: "+salesFCT +" vs "+salesSFCT)
  }

  "File trees" should "be synchronized" in {
    val delta = new Delta(spark.sparkContext.hadoopConfiguration)
    delta.synchronize(source, target)
    refreshMetadata(t.db, "STORE_SALES_FCT")(spark)
  }

  "File trees" should "be the the same after the test" in {
    val s = listLevel(fs, new Path(source)).map(x => x.path.replace(source, target))
    val t = listLevel(fs, new Path(target)).map(_.path)
    assert(s.diff(t).isEmpty && t.diff(s).isEmpty)
  }

  "After synchronization FCT sales" should "be the same as SFCT" in {
    val afterSalesFCT = spark.table(t.d + "STORE_SALES_FCT").agg(sum("sales_usd_amt").
      alias("sales")).collect().map(_.getAs[Double]("sales")).head.toLong
    assert(afterSalesFCT == salesSFCT, "Sales should be the same in SFCT and FCT after synchro - " + salesSFCT + " vs " + afterSalesFCT)
  }

  "After synchronization SFCT sales" should "remain the same as in SFCT initially" in {
    refreshMetadata(t.db, "STORE_SALES_SFCT")(spark)
    val afterSalesSFCT = spark.table(t.d + "STORE_SALES_FCT").agg(sum("sales_usd_amt").
      alias("sales")).collect().map(_.getAs[Double]("sales")).head.toLong
    assert(afterSalesSFCT == salesSFCT, "Sales should be the same in SFCT and FCT after synchro - " + salesSFCT + " vs " + afterSalesSFCT)
  }


}
