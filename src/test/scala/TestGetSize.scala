import org.scalatest.FlatSpec
import com.pg.bigdata.octopufs.fs.getSize
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
class TestGetSize extends FlatSpec{
  implicit val spark: SparkSession = SparkSession.builder().
    appName("NAS_").
    master("local").
    getOrCreate()
  val currentDirectory = new java.io.File(".").getCanonicalPath
  val path = currentDirectory+"/data"
  implicit val conf = spark.sparkContext.hadoopConfiguration

  "getSize" should "return size of data folder" in {
    getSize(path)
  }

  "getSize" should "return actual number of elements and size of data folder" in {
    getSize(path, false).sizes.foreach(println)
  }

  "getSize" should "return the same size despite if files were collapsed or not" in {
    val localPath = "file:"+path
    assert(getSize(path, false).getSizeOfPath(localPath) == getSize(path, true).getSizeOfPath(localPath))
  }
}
