import com.pg.bigdata.octopufs.Promotor
import com.pg.bigdata.octopufs.fs._
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FlatSpec}

class TestSubFolderMove extends FlatSpec with BeforeAndAfterAll {
  implicit val spark: SparkSession = SparkSession.builder().
    appName("NAS_").
    master("local").
    getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  implicit val c = spark.sparkContext.hadoopConfiguration

  val (startPath, destPath, subFoldersToBeCopied, dummyFile, simulatedFolderToBeKept) = TestUtils.createFolderStructureForTest("FolderMoveOverwrite")

  val fs = getFileSystem(c, startPath)

  Promotor.moveSelectedSubFolders(startPath, destPath, subFoldersToBeCopied)

  "Source folder" should "contain other folders after the move (2 ouf of 5 are to me moved)" in {
    assert(fs.listStatus(new Path(startPath)).length == 3,"start folder has 5 subfolders. 2 of them were moved. 3 should remain")
  }

  "After partition copy destination foler" should "contain folder which should not be impacted" in {
    val destFolders = fs.listStatus(new Path(destPath)).map(_.getPath.getName)
    destFolders.foreach(println)
    assert(destFolders.filter(_ != simulatedFolderToBeKept.getName).sameElements(subFoldersToBeCopied))
  }

  "After subfolders move, folder" should "contain dummy file" in {
    assert(fs.exists(dummyFile))
    assert(fs.exists(simulatedFolderToBeKept))
  }

  "Folders on source side should no longer exist" should "match the source" in {
    val foldersExistInSource = subFoldersToBeCopied.map(f => fs.exists(new Path(s"$startPath/$f"))).reduce(_ || _)
    assert(!foldersExistInSource)
  }
  "Folders on target side" should "exist and should not be empty" in {
    val y = subFoldersToBeCopied.map(f => listLevel(fs, new Path(s"$destPath/$f")).map(_.path.replace(destPath, ""))).reduce(_ ++ _)
    assert(y.length > 4, "there should be at least 2 folders and 2 files")

  }

  override def afterAll() = {
    fs.delete(new Path(startPath), true)
    fs.delete(new Path(destPath), true)
  }
}
