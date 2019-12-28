package com.pg.bigdata.utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.permission.{AclEntry, AclEntryScope, AclEntryType, FsAction}
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.{FileSystem, Path}




object ACLs extends Serializable {

def modifyTableACLs(db: String, tableName: String, newPermission: FSPermission)(implicit spark: SparkSession, confEx: Configuration): Int = {
  import collection.JavaConverters._

  val sdConf = new ConfigSerDeser(confEx)
  //sdConf.get.set("fs.azure.account.key.adls2nas001.dfs.core.windows.net","xxx")
  val loc = Promotor.getTableLocation(db,tableName)(spark)
  val files = Promotor.getListOfTableFiles(db,tableName)(spark)

  //first, set up default privs on a folder which table was defined on top of
  Promotor.getFileSystem(confEx, loc).modifyAclEntries(
    new Path(Promotor.getRelativePath(loc)),
    Seq(ACLs.getAclEntry(newPermission.getDefaultLevelPerm())).asJava)


  println(files(0))
  println("Files to process: "+files.length)


  spark.sparkContext.parallelize(files,30).mapPartitions(part => {
    val eeeeeeee = sdConf.get()
    val y = ACLs.getAclEntry(newPermission)
    val fs  = Promotor.getFileSystem(eeeeeeee, loc)
    part.map(x => {
      fs.modifyAclEntries(new Path(x),Seq(y).asJava)
      true
    })
  }
  ).collect.length
}

  def getAclEntry(p: FSPermission): AclEntry = {
    val ptype = if(p.scope == p.USER) AclEntryType.USER
    else if(p.scope == p.GROUP) AclEntryType.GROUP
    else if(p.scope == p.OTHER)  AclEntryType.OTHER
    else AclEntryType.MASK

    val perm = FsAction.getFsAction(p.permission)
    if(perm == null)
      throw new Exception("Provided permission "+p.permission+" is not valid for FsAction")
    val level = AclEntryScope.valueOf(p.level)
    var x = new AclEntry.Builder()
    val y = x.
      setType(ptype).
      setPermission(perm).
      setScope(level).
      setName(p.granteeObjectId).build()
    println(y.toString)
    y
  }

  case class FSPermission(scope: String, permission: String, level: String, granteeObjectId: String){
    val USER: String = "user"
    val GROUP: String = "group"
    val OTHER: String = "other"
    val MASK: String = "mask"
    def getDefaultLevelPerm(): ACLs.FSPermission ={
      FSPermission(scope,permission,"DEFAULT",granteeObjectId)
    }
  }
  //def modifyACL()
}
