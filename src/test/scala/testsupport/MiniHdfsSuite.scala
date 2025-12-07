package testsupport

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hdfs.MiniDFSCluster
import org.scalatest.BeforeAndAfterAll

trait MiniHdfsSuite extends BeforeAndAfterAll { self: org.scalatest.Suite =>
  @transient private var cluster: MiniDFSCluster = _
  @transient protected var hdfsConf: Configuration = _
  protected var hdfsUri: String = _
  protected var hdfsFs: FileSystem = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    val conf = new Configuration()
    conf.setBoolean("dfs.namenode.acls.enabled", true)
    conf.setBoolean("dfs.permissions.enabled", false)
    conf.set("dfs.replication", "1")
    conf.set("fs.defaultFS", "hdfs://localhost:8020")
    // Configure group mapping to avoid OS group lookup issues during tests
    conf.set("hadoop.security.group.mapping", "org.apache.hadoop.security.ShellBasedUnixGroupsMapping")
    conf.set("dfs.permissions.superusergroup", "supergroup")
    conf.set("hadoop.user.group.static.mapping", "testuser=supergroup")
    val builder = new MiniDFSCluster.Builder(conf)
    cluster = builder.build()
    cluster.waitClusterUp()
    hdfsConf = conf
    hdfsUri = cluster.getFileSystem.getUri.toString
    hdfsFs = cluster.getFileSystem
  }

  override protected def afterAll(): Unit = {
    try {
      if (cluster != null) cluster.shutdown(true)
    } finally {
      super.afterAll()
    }
  }
}
