import com.pg.bigdata.octopufs.fs.getFileSystem
import org.apache.hadoop.fs.Path
import org.scalatest.flatspec.AnyFlatSpec
import testsupport.{HadoopSimpleAuth, LocalSparkSuite, TempFsHelper}

class TestSynchronizeAcls extends AnyFlatSpec with LocalSparkSuite with HadoopSimpleAuth with TempFsHelper {
  import com.pg.bigdata.octopufs.acl.AclManager

  "Setting empty ACLs" should "not crash on local file scheme" in {
    val base = tempDir("syncacls")
    val baseUri = base.toUri.toString
    implicit val conf = spark.sparkContext.hadoopConfiguration
    val fs = getFileSystem(conf, baseUri)
    val cursedFolder = new Path(baseUri + "/folderNoAcls")
    val regularFolder = new Path(baseUri + "/regularFolder")
    fs.mkdirs(cursedFolder)
    fs.mkdirs(regularFolder)
    // Local file:// may not support ACL operations; this verifies no exceptions in synchronizeAcls when ACLs are absent
    AclManager.synchronizeAcls(regularFolder.toString, cursedFolder.toString)
    fs.delete(cursedFolder, true)
    fs.delete(regularFolder, true)
  }
}
