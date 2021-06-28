import com.pg.bigdata.octopufs.Promotor
import com.pg.bigdata.octopufs.fs._
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FlatSpec}

class TestSubFolderCopyOverwrite extends FlatSpec with BeforeAndAfterAll {
  implicit val spark: SparkSession = SparkSession.builder().
    appName("NAS_").
    master("local").
    getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  implicit val c = spark.sparkContext.hadoopConfiguration

  val (startPath, destPath, subFoldersToBeCopied, dummyFile, simulatedFolderToBeKept) = TestUtils.createFolderStructureForTest("FolderCopyOverwrite")
  val fs = getFileSystem(c, startPath)
  Promotor.copyOverwriteSelectedSubfoldersContent(startPath, destPath, subFoldersToBeCopied)

  "Source folder" should "contain more than 2 subfolders (precheck)" in {
    assert(fs.listStatus(new Path(startPath)).length > 2, "If source does not have more than 2 sub-folders, than sub-folders filtering test is limited and may be false positive")
  }

  "After partition copy, STORE_SALES_SFCT" should "contain new partition as well as the other ones which existed there before" in {
    val destFolders = fs.listStatus(new Path(destPath)).map(_.getPath.getName)
    destFolders.foreach(println)
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
    println(x.diff(y).mkString(", "))
    println("------------")
    println(y.diff(x).mkString(", "))
    assert(x.sameElements(y))
  }

  override def afterAll() = {
    fs.delete(new Path(startPath), true)
    fs.delete(new Path(destPath), true)
  }
}
