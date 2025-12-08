package testsupport

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll

trait LocalSparkSuite extends BeforeAndAfterAll { self: org.scalatest.Suite =>
  @transient protected var _spark: SparkSession = _
  def spark: SparkSession = _spark

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    _spark = SparkSession.builder()
      .master("local[2]")
      .appName("octopufs-tests")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.sql.codegen.wholeStage", "false")
      .getOrCreate()
    this match {
      case h: HadoopSimpleAuth =>
        val scConf = _spark.sparkContext.hadoopConfiguration
        val auth = h.hadoopConf.get("hadoop.security.authentication")
        if (auth != null) scConf.set("hadoop.security.authentication", auth)
      case _ =>
    }
  }

  override protected def afterAll(): Unit = {
    try Option(_spark).foreach(_.stop())
    finally super.afterAll()
  }
}
