import com.pg.bigdata.octopufs.Promotor
import com.pg.bigdata.octopufs.fs._
import org.apache.hadoop.fs.Path
import org.scalatest.flatspec.AnyFlatSpec
import testsupport.{HadoopSimpleAuth, LocalSparkSuite, TempFsHelper}

class TestSubFolderCopyOverwrite extends AnyFlatSpec with LocalSparkSuite with HadoopSimpleAuth with TempFsHelper {
  implicit val c = spark.sparkContext.hadoopConfiguration
  private val base = tempDir("subfolders-copy-overwrite").toString
  private val (startPath, destPath, subFoldersToBeCopied, dummyFile, simulatedFolderToBeKept) = TestUtils.createFolderStructureForTest("FolderCopyOverwrite")(c)
  private val newPath = base + "/someFolderWhichDoesNotExist"
  private val fs = getFileSystem(c, startPath)
  implicit val conf = spark.sparkContext.hadoopConfiguration
  implicit val sparkSession = spark

  "Copy operation" should "succeed" in {
    Promotor.copyOverwriteSelectedSubfoldersContent(startPath, destPath, subFoldersToBeCopied)
  }

  "Copy operation to path which does not exist" should "succeed" in {
    Promotor.copyOverwriteSelectedSubfoldersContent(startPath, newPath, subFoldersToBeCopied)
  }

  "Source folder" should "contain more than 2 subfolders (precheck)" in {
    assert(fs.listStatus(new Path(startPath)).length > 2, "If source does not have more than 2 sub-folders, than sub-folders filtering test is limited and may be false positive")
  }

  "After subfolder copy" should "contain new partition as well as the other ones which existed there before" in {
    val destFolders = fs.listStatus(new Path(destPath)).map(_.getPath.getName)
    assert(destFolders.filter(_ != simulatedFolderToBeKept.getName).sameElements(subFoldersToBeCopied))
  }

  "After subfolders move, folder" should "contain dummy file" in {
    assert(fs.exists(dummyFile))
    assert(fs.exists(simulatedFolderToBeKept))
  }

  "Content after copy" should "match the source" in {
    val x = subFoldersToBeCopied.map(f => listLevel(fs, new Path(s"$startPath/$f")).map(_.path.replace(startPath, ""))).reduce(_ ++ _)
    val y = subFoldersToBeCopied.map(f => listLevel(fs, new Path(s"$destPath/$f")).map(_.path.replace(destPath, ""))).reduce(_ ++ _)
    assert(x.length > 0, "source folders should not be empty for the test to make sense")
    assert(x.length == y.length)
    assert(x.sameElements(y))
  }
}
