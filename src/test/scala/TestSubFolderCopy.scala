import com.pg.bigdata.octopufs.Promotor
import com.pg.bigdata.octopufs.fs._
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import testsupport._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TestSubFolderCopy extends AnyFlatSpec with Matchers with LocalSparkSuite with HadoopSimpleAuth with TempFsHelper { self: org.scalatest.Suite =>
  implicit def ss: SparkSession = spark
  implicit def c: org.apache.hadoop.conf.Configuration = spark.sparkContext.hadoopConfiguration

  private var base: String = _
  private var startPath: String = _
  private var destPath: String = _
  private var fs: org.apache.hadoop.fs.FileSystem = _
  private var subFoldersToBeCopied: Array[String] = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    base = tempDir("subfolder-copy").toString
    startPath = base + "/FolderCopyTest"
    destPath = base + "/copysubfolderstarget"
    TestUtils.createRandomFolderStructure(startPath)
    fs = getFileSystem(spark.sparkContext.hadoopConfiguration, startPath)
    subFoldersToBeCopied = fs.listStatus(new Path(startPath)).filter(_.isDirectory).take(2).map(_.getPath.getName)
  }

  it should "copy selected subfolders content" in {
    Promotor.copySelectedSubFoldersContent(startPath, destPath, subFoldersToBeCopied)(spark)
  }

  "Source folder" should "contain more than 2 subfolders (precheck)" in {
    assert(fs.listStatus(new Path(startPath)).length > 2, "If source does not have more than 2 sub-folders, than sub-folders filtering test is limited and may be false positive")
  }

  "After partition copy, STORE_SALES_SFCT" should "contain new partition as well as the other ones which existed there before" in {
    assert(fs.listStatus(new Path(destPath)).map(_.getPath.getName).sameElements(subFoldersToBeCopied))
  }

  "Content after copy" should "match the source" in {
    val x = subFoldersToBeCopied.map(f => listLevel(fs, new Path(s"$startPath/$f")).map(_.path.replace(startPath,""))).reduce(_ ++ _)
    val y = subFoldersToBeCopied.map(f => listLevel(fs, new Path(s"$destPath/$f")).map(_.path.replace(destPath,""))).reduce(_ ++ _)
    assert(x.length>0, "source folders should not be empty for the test to make sense")
    assert(x.length == y.length)
    println(x.diff(y).mkString(", "))
    println("------------")
    println(y.diff(x).mkString(", "))
    assert(x.sameElements(y))
  }

  override protected def afterAll(): Unit = {
    fs.delete(new Path(startPath), true)
    fs.delete(new Path(destPath), true)
  }
}
