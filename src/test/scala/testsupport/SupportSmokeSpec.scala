package testsupport

import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class SupportSmokeSpec extends AnyFunSuite with Matchers with LocalSparkSuite with HadoopSimpleAuth with TempFsHelper {
  test("spark starts and writes to local temp dir") {
    val tmp = tempDir("support-smoke")
    implicit val sparkSession: SparkSession = spark
    import sparkSession.implicits._
    val df = spark.createDataFrame(Seq((1, "a"), (2, "b")).map{case (i, s) => (i, s)}).toDF("id", "val")
    val out = tmp.resolve("out").toString
    df.write.mode("overwrite").parquet(out)
    val back = spark.read.parquet(out).orderBy("id")
    back.count() shouldBe 2
  }

  test("spark hadoop conf is SIMPLE auth") {
    val auth = spark.sparkContext.hadoopConfiguration.get("hadoop.security.authentication")
    auth shouldBe "simple"
  }
}
