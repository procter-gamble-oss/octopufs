import com.pg.bigdata.octopufs.SafetyFuse
import com.pg.bigdata.octopufs.fs._
import org.scalatest.flatspec.AnyFlatSpec
import testsupport.{HadoopSimpleAuth, LocalSparkSuite, TempFsHelper}

class SafetyFuseTest extends AnyFlatSpec with LocalSparkSuite with HadoopSimpleAuth with TempFsHelper {
  implicit val c = spark.sparkContext.hadoopConfiguration
  private val base = tempDir("safety")
  implicit val fs = getFileSystem(c, base.toUri.toString)
  private val transaction = new SafetyFuse(base.toString, "test")

  "transaction" should "toggle in-progress state" in {
    assert(!transaction.isInProgress(), "transaction not started")
    transaction.startTransaction()
    assert(transaction.isInProgress(), "check if transaction in progress")
    transaction.endTransaction()
    assert(!transaction.isInProgress(), "transaction completed (not in progress)")
  }
}
