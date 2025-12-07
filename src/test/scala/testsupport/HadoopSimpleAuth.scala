package testsupport

import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.conf.Configuration

trait HadoopSimpleAuth { self: LocalSparkSuite =>
  lazy val hadoopConf: Configuration = {
    val conf = new Configuration()
    conf.set("hadoop.security.authentication", "simple")
    UserGroupInformation.setConfiguration(conf)
    val osUser = System.getProperty("user.name", "testuser")
    // Avoid JAAS Subject-based current user; use HADOOP_USER_NAME
    System.setProperty("HADOOP_USER_NAME", osUser)
    conf
  }
}
