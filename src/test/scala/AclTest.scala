
import com.pg.bigdata.utils.helpers.implicits._
import com.pg.bigdata.utils.fs._
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.permission.{AclEntry, AclEntryScope, AclEntryType, FsAction}
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FlatSpec}

import scala.collection.JavaConverters._
import scala.util.Random

class AclTest extends FlatSpec with BeforeAndAfterAll {

  import com.pg.bigdata.utils.acl.AclManager

  implicit val spark: SparkSession = SparkSession.builder().
    appName("NAS_").
    master("local").
    getOrCreate()

  "This" should "never fail" in {
    assert(1==1)
  }


    val basePath = "hdfs://localhost:8020/"
    implicit val conf = spark.sparkContext.hadoopConfiguration
    val fs = getFileSystem(conf, basePath)
    val toTakeAclsFrom = basePath + "pattern"
    val toApplyOn = basePath + "hereIsTheNewOne"

    fs.delete(new Path(toTakeAclsFrom), true)
    fs.delete(new Path(toApplyOn), true)

    val aclsForBase = List(new AclEntry.Builder().
      setType(AclEntryType.OTHER).
      setPermission(FsAction.getFsAction("rwx")).
      setScope(AclEntryScope.DEFAULT).build(),
      new AclEntry.Builder().
        setType(AclEntryType.GROUP).
        setPermission(FsAction.getFsAction("rwx")).
        setScope(AclEntryScope.DEFAULT).build(),
      new AclEntry.Builder().
        setType(AclEntryType.USER).
        setPermission(FsAction.getFsAction("rwx")).
        setScope(AclEntryScope.DEFAULT).
        setName("jacektokar").build())

    val aclForRoot = List(
      new AclEntry.Builder().
        setType(AclEntryType.OTHER).
        setPermission(FsAction.getFsAction("rwx")).
        setScope(AclEntryScope.DEFAULT).build(),
      new AclEntry.Builder().
        setType(AclEntryType.GROUP).
        setPermission(FsAction.getFsAction("rwx")).
        setScope(AclEntryScope.DEFAULT).build(),
      new AclEntry.Builder().
        setType(AclEntryType.USER).
        setPermission(FsAction.getFsAction("r--")).
        setScope(AclEntryScope.DEFAULT).
        setName("grzyb").build())

    val (files, folders) = TestUtils.createRandomFolderStructure(toTakeAclsFrom)

    //setting default acls for all childeren of root folder
    fs.setAcl(new Path(basePath), aclsForBase.asJava)

    fs.setAcl(new Path(toTakeAclsFrom), aclForRoot.asJava)
    val toTakeAclsFromFiles = TestUtils.makeRandomCopyOfFiles(fs, files, toTakeAclsFrom, toApplyOn)
    val toApplyAclsOnFiles = new Random().shuffle(toTakeAclsFromFiles).take((toTakeAclsFromFiles.length * 0.5).toInt).map(_.replace("pattern", "hereIsTheNewOne"))
    val targetFolders = toApplyAclsOnFiles.map(x => new Path(x).getParent.toUri.toString)

    def buildRandomAclForPath(path: String, scopeType: String = "ACCESS") = {
      val scope = if (scopeType == "ACCESS") "ACCESS" else "DEFAULT"
      val typeOfAccess = List("r", "-")(new Random().nextInt(2)) +
        List("w", "-")(new Random().nextInt(2)) +
        List("x", "-")(new Random().nextInt(2))
      val p = AclManager.FsPermission("user", typeOfAccess, "ACCESS", "jacektokar") // path.split("/").last)
      AclManager.getAclEntry(p)

    }

    println("target folders:")
    println(targetFolders.mkString("\n"))


    println("Setting folders ACLs")

    import scala.util.Random
    //new Random().shuffle(folders.toList).take((folders.length*0.8).toInt).map(x => fs.modifyAclEntries(new Path(x),List(buildRandomAclForPath(x, "DEFAULT")).asJava))
    new Random().shuffle(folders.toList).take((folders.length * 0.8).toInt).map(x => fs.modifyAclEntries(new Path(x), List(buildRandomAclForPath(x, "ACCESS")).asJava))
    new Random().shuffle(files.toList).take((files.length * 0.8).toInt).map(x => fs.modifyAclEntries(new Path(x), List(buildRandomAclForPath(x, "ACCESS")).asJava))
    toApplyAclsOnFiles.map(x => fs.modifyAclEntries(new Path(x), List(buildRandomAclForPath(x, "ACCESS")).asJava))
    targetFolders.map(x => fs.modifyAclEntries(new Path(x), List(buildRandomAclForPath(x, "ACCESS")).asJava))
    // targetFolders.map(x => fs.modifyAclEntries(new Path(x),List(buildRandomAclForPath(x, "DEFAULT")).asJava))

  "File trees" should "be the different before the test" in {
    assert(listLevel(fs, Array(new Path(toTakeAclsFrom))) != listLevel(fs, Array(new Path(toApplyOn))))
  }

  "ACLs before test" should "be the different in both folder trees" in {
    val initAcls = folders.sorted.map(x => fs.getAclStatus(new Path(x))).toList
    val targetAcls = targetFolders.sorted.map(x => fs.getAclStatus(new Path(x)))
    assert(initAcls != targetAcls)
  }

  AclManager.synchronizeAcls(toApplyOn, toTakeAclsFrom)

  "ACLs" should "be the same" in {
    val initAcls = folders.sorted.map(x => fs.getAclStatus(new Path(x))).toList
    val targetAcls = targetFolders.sorted.map(x => fs.getAclStatus(new Path(x)))
    assert(initAcls != targetAcls)
  }

  "ACLs of top folders" should "be the same" in {
    assert(fs.getAclStatus(new Path(toTakeAclsFrom)) == fs.getAclStatus(new Path(toApplyOn)))
    //assert(initAcls != targetAcls)
  }
  "File trees" should "be the different AFTER the test" in {
    assert(listLevel(fs, Array(new Path(toTakeAclsFrom))) != listLevel(fs, Array(new Path(toApplyOn))))
    println("cleanup")
    folders.map(x => fs.delete(new Path(x), true))
    folders.map(x => fs.delete(new Path(basePath + "hereIsTheNewOne"), true))
  }

}
