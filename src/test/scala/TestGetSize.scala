import org.scalatest.FlatSpec
import com.pg.bigdata.octopufs.fs.getSize
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
class TestGetSize extends FlatSpec{
  implicit val spark: SparkSession = SparkSession.builder().
    appName("NAS_").
    master("local").
    getOrCreate()

  "getSize" should "return size of data folder" in {
    val currentDirectory = new java.io.File(".").getCanonicalPath
    implicit val conf = spark.sparkContext.hadoopConfiguration
    getSize(currentDirectory+"/data" )
  }
}
