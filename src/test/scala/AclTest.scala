import com.pg.bigdata.octopufs.fs._
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.permission.{AclEntry, AclEntryScope, AclEntryType, FsAction}
import org.scalatest.flatspec.AnyFlatSpec
import testsupport.{HadoopSimpleAuth, LocalSparkSuite, MiniHdfsSuite}

class AclTest extends AnyFlatSpec with LocalSparkSuite with HadoopSimpleAuth with MiniHdfsSuite {
  import com.pg.bigdata.octopufs.acl.AclManager
  import scala.jdk.CollectionConverters._

  private def acl(u: String, perm: String, scope: AclEntryScope): AclEntry =
    new AclEntry.Builder().setType(AclEntryType.USER).setPermission(FsAction.getFsAction(perm)).setScope(scope).setName(u).build()

  private def assertAclEqual(fs: org.apache.hadoop.fs.FileSystem, p1: Path, p2: Path): Unit = {
    val a1 = fs.getAclStatus(p1)
    val a2 = fs.getAclStatus(p2)
    assert(a1.getEntries == a2.getEntries && a1.getOwner == a2.getOwner && a1.getGroup == a2.getGroup)
  }

  "ACCESS ACL synchronization" should "match ACLs across trees" in {
    implicit val conf = hdfsConf
    val fs = hdfsFs

    val src = new Path(hdfsUri + "/pattern")
    val dst = new Path(hdfsUri + "/hereIsTheNewOne")
    val srcA = new Path(src, "a")
    val srcB = new Path(src, "b")
    val dstA = new Path(dst, "a")
    val dstB = new Path(dst, "b")
    val srcAf1 = new Path(srcA, "f1.txt")
    val srcAf2 = new Path(srcA, "f2.txt")
    val srcBf1 = new Path(srcB, "f1.txt")
    val dstAf1 = new Path(dstA, "f1.txt")
    val dstAf2 = new Path(dstA, "f2.txt")
    val dstBf1 = new Path(dstB, "f1.txt")

    List(srcA, srcB, dstA, dstB).foreach(fs.mkdirs)
    List(srcAf1, srcAf2, srcBf1, dstAf1, dstAf2, dstBf1).foreach(f => fs.createNewFile(f))

    val baseAccess = List(
      new org.apache.hadoop.fs.permission.AclEntry.Builder().setType(AclEntryType.USER).setPermission(FsAction.ALL).setScope(AclEntryScope.ACCESS).build(),
      new org.apache.hadoop.fs.permission.AclEntry.Builder().setType(AclEntryType.GROUP).setPermission(FsAction.READ_EXECUTE).setScope(AclEntryScope.ACCESS).build(),
      new org.apache.hadoop.fs.permission.AclEntry.Builder().setType(AclEntryType.OTHER).setPermission(FsAction.READ_EXECUTE).setScope(AclEntryScope.ACCESS).build()
    )
    fs.setAcl(src, (baseAccess ++ List(acl("testuser", "r--", AclEntryScope.ACCESS))).asJava)
    fs.setAcl(dst, (baseAccess ++ List(acl("anotheruser", "r--", AclEntryScope.ACCESS))).asJava)

    assert(fs.getAclStatus(src).getEntries != fs.getAclStatus(dst).getEntries)

    AclManager.synchronizeAcls(dst.toString, src.toString)

    assertAclEqual(fs, src, dst)
  }

  "DEFAULT ACL synchronization" should "apply directory defaults" in {
    implicit val conf = hdfsConf
    val fs = hdfsFs

    val src = new Path(hdfsUri + "/pattern")
    val dst = new Path(hdfsUri + "/hereIsTheNewOne")
    val child = new Path(dst, "child")
    fs.mkdirs(src); fs.mkdirs(dst); fs.mkdirs(child)

    val baseDefault = List(
      new org.apache.hadoop.fs.permission.AclEntry.Builder().setType(AclEntryType.USER).setPermission(FsAction.ALL).setScope(AclEntryScope.DEFAULT).build(),
      new org.apache.hadoop.fs.permission.AclEntry.Builder().setType(AclEntryType.GROUP).setPermission(FsAction.READ_EXECUTE).setScope(AclEntryScope.DEFAULT).build(),
      new org.apache.hadoop.fs.permission.AclEntry.Builder().setType(AclEntryType.OTHER).setPermission(FsAction.READ_EXECUTE).setScope(AclEntryScope.DEFAULT).build()
    )
    fs.setAcl(src, (baseDefault ++ List(acl("testuser", "r--", AclEntryScope.DEFAULT))).asJava)
    fs.setAcl(dst, baseDefault.asJava)

    AclManager.synchronizeAcls(dst.toString, src.toString)

    val sDef = fs.getAclStatus(src)
    val dDef = fs.getAclStatus(dst)
    assert(sDef.getEntries == dDef.getEntries)
  }

  "Idempotent synchronization" should "produce no further changes" in {
    implicit val conf = hdfsConf
    val fs = hdfsFs

    val src = new Path(hdfsUri + "/pattern")
    val dst = new Path(hdfsUri + "/hereIsTheNewOne")
    fs.mkdirs(src); fs.mkdirs(dst)
    val baseAccess = List(
      new org.apache.hadoop.fs.permission.AclEntry.Builder().setType(AclEntryType.USER).setPermission(FsAction.ALL).setScope(AclEntryScope.ACCESS).build(),
      new org.apache.hadoop.fs.permission.AclEntry.Builder().setType(AclEntryType.GROUP).setPermission(FsAction.READ_EXECUTE).setScope(AclEntryScope.ACCESS).build(),
      new org.apache.hadoop.fs.permission.AclEntry.Builder().setType(AclEntryType.OTHER).setPermission(FsAction.READ_EXECUTE).setScope(AclEntryScope.ACCESS).build()
    )
    fs.setAcl(src, baseAccess.asJava)

    AclManager.synchronizeAcls(dst.toString, src.toString)
    val first = fs.getAclStatus(dst)
    AclManager.synchronizeAcls(dst.toString, src.toString)
    val second = fs.getAclStatus(dst)
    assert(first.getEntries == second.getEntries)
  }
}
