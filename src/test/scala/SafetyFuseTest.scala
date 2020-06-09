import com.pg.bigdata.octopufs.{Promotor, SafetyFuse}
import com.pg.bigdata.octopufs.fs._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit
import org.scalatest.FlatSpec

class SafetyFuseTest extends FlatSpec {
  implicit val spark: SparkSession = SparkSession.builder().
    appName("NAS_").
    master("local[1]").
    getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  implicit val c = spark.sparkContext.hadoopConfiguration
  implicit val fs = getFileSystem(c,"file://data")
  val transaction = new SafetyFuse("data/testfield","test")



  "transaction" should "work fine :)" in {
    assert(!transaction.isInProgress(), "transaction not started")
    transaction.startTransaction()
    assert(transaction.isInProgress(), "check if transaction in progress")
    transaction.endTransaction()
    assert(!transaction.isInProgress(), "transaction completed (not in progress)")
  }

}


