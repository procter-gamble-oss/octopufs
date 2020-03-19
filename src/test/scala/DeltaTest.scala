import org.scalatest.FlatSpec
import org.apache.spark.sql.functions._
import com.pg.bigdata.utils.Delta
import com.pg.bigdata.utils.metastore._
import org.apache.spark.sql.SparkSession

class DeltaTest extends FlatSpec {

  val t = new TestUtils("DeltaTest")
  t.setupTestEnv()
  val basePath = "file:/Users/jacektokar/IdeaProjects/Promoter/data"

  implicit val spark: SparkSession = t.spark
  val salesSFCT = spark.table(t.d+"STORE_SALES_SFCT").agg(sum("sales_usd_amt").
    alias("sales")).collect().map(_.getAs[Double]("sales")).head.toLong
  val salesFCT = spark.table(t.d+"STORE_SALES_FCT").agg(sum("sales_usd_amt").
    alias("sales")).collect().map(_.getAs[Double]("sales")).head.toLong

  "init sales from SFCT and FCT" should "be different (before the test)" in {

    assert(salesFCT!= salesSFCT, "Sales should be different in FCT and SFCT - "+salesFCT+" vs "+salesSFCT)
  }

  val source = getTableLocation(t.db,"STORE_SALES_SFCT") //basePath+"/testfield/DeltaTest/STORE_SALES_SFCT"
  val target = getTableLocation(t.db,"STORE_SALES_FCT") //basePath+"/testfield/DeltaTest/STORE_SALES_FCT"

  val delta = new Delta(spark.sparkContext.hadoopConfiguration)
  delta.synchronize(source,target,1,2,3)
  refreshMetadata(t.db, "STORE_SALES_FCT")(spark)

  "After synchronization FCT sales" should "be the same as SFCT" in {

    val afterSalesFCT = spark.table(t.d+"STORE_SALES_FCT").agg(sum("sales_usd_amt").
      alias("sales")).collect().map(_.getAs[Double]("sales")).head.toLong
    assert(afterSalesFCT == salesSFCT, "Sales should be the same in SFCT and FCT after synchro - "+salesSFCT+" vs "+afterSalesFCT)
  }
}
