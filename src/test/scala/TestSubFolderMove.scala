import com.pg.bigdata.octopufs.Promotor
import com.pg.bigdata.octopufs.fs._
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import testsupport._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TestSubFolderMove extends AnyFlatSpec with Matchers with LocalSparkSuite with HadoopSimpleAuth with TempFsHelper { self: org.scalatest.Suite =>
  implicit def ss: SparkSession = spark
  implicit def c: org.apache.hadoop.conf.Configuration = spark.sparkContext.hadoopConfiguration

  private var startPath: String = _
  private var destPath: String = _
  private var subFoldersToBeCopied: Array[String] = _
  private var dummyFile: org.apache.hadoop.fs.Path = _
  private var simulatedFolderToBeKept: org.apache.hadoop.fs.Path = _

  private var startPath2: String = _
  private var destPath2: String = _
  private var subFoldersToBeCopied2: Array[String] = _

  private var newPath: String = _
  private var fs: org.apache.hadoop.fs.FileSystem = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    val base = tempDir("subfolder-move").toString
    val s1 = TestUtils.createFolderStructureForTest("FolderMoveOverwrite")
    startPath = s1._1; destPath = s1._2; subFoldersToBeCopied = s1._3; dummyFile = s1._4; simulatedFolderToBeKept = s1._5
    val s2 = TestUtils.createFolderStructureForTest("FolderMoveOverwrite2")
    startPath2 = s2._1; destPath2 = s2._2; subFoldersToBeCopied2 = s2._3
    newPath = base + "/someFolderWhichDoesNotExist2"
    fs = getFileSystem(c, startPath)
  }

  "Copy operation" should "succeed" in {
    Promotor.moveSelectedSubFolders(startPath, destPath, subFoldersToBeCopied)(spark)
  }

  "Copy operation to path which does not exist" should "succeed" in {
    Promotor.moveSelectedSubFolders(startPath2, newPath, subFoldersToBeCopied2)(spark)
  }


  "Source folder" should "contain other folders after the move (2 ouf of 5 are to me moved)" in {
    assert(fs.listStatus(new Path(startPath)).length == 3, "start folder has 5 subfolders. 2 of them were moved. 3 should remain")
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

  override protected def afterAll(): Unit = {
    fs.delete(new Path(startPath), true)
    fs.delete(new Path(destPath), true)
    fs.delete(new Path(newPath), true)

  }
}
