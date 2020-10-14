import com.pg.bigdata.octopufs.fs.getFileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.scalatest.{FlatSpec, TestSuite}

class TestSynchronizeAcls extends FlatSpec {
  import com.pg.bigdata.octopufs.acl.AclManager

  implicit val spark: SparkSession = SparkSession.builder().
    appName("NAS_").
    master("local").
    getOrCreate()

  "Setting empty ACLs" should "never fail" in {
    val basePath = "hdfs://localhost:8020/"
    implicit val conf = spark.sparkContext.hadoopConfiguration
    val fs = getFileSystem(conf, basePath)
    val cursedFolder = new Path("/folderNoAcls")
    val regularFolder = new Path("/regularFolder")
    fs.mkdirs(cursedFolder)
    fs.mkdirs(regularFolder)
    fs.removeAcl(cursedFolder)
    fs.removeAcl(regularFolder)
    AclManager.synchronizeAcls("hdfs://localhost:8020/regularFolder", "hdfs://localhost:8020/folderNoAcls")
    fs.delete(cursedFolder,true)
    fs.delete(regularFolder,true)
  }


}
