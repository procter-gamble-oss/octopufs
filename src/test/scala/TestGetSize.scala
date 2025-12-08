import org.scalatest.flatspec.AnyFlatSpec
import testsupport.{HadoopSimpleAuth, LocalSparkSuite, TempFsHelper}
import com.pg.bigdata.octopufs.fs.getSize
import org.apache.hadoop.conf.Configuration

class TestGetSize extends AnyFlatSpec with LocalSparkSuite with HadoopSimpleAuth with TempFsHelper {
  private val base = tempDir("getsize")
  private val path = base.toString
  implicit val conf: Configuration = spark.sparkContext.hadoopConfiguration

  "getSize" should "return size of temp folder" in {
    getSize(path)
  }

  "getSize" should "return elements and size of temp folder" in {
    getSize(path, false).sizes.foreach(println)
  }

  "getSize" should "return the same size whether collapsed or not" in {
    val localPath = base.toUri.toString
    assert(getSize(path, false).getSizeOfPath(localPath) == getSize(path, true).getSizeOfPath(localPath))
  }
}
