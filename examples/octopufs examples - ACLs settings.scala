// Databricks notebook source
// DBTITLE 1,Modify folder ACLs (including files/subfolders in that folder)
import com.pg.bigdata.octopufs.acl.AclManager
implicit val conf = spark.sparkContext.hadoopConfiguration

val acl = AclManager.FsPermission("user", "r--","ACCESS","jacek") //set read only permission for user jacek ("jacek" shoulld be replaced by object ID of the user from azure active directory)
//val acl = AclManager.FsPermission("group", "r--","ACCESS","securityGroupID") //set read only permission for group securityGroupID ("securityGroupID" shoulld be replaced by object ID of the group from azure active directory)
AclManager.modifyFolderAcl("abfss://container@adlsname.dfs.core.windows.net/j/IRI/", acl) //function recently renamed from modifyFolderACLs

// COMMAND ----------

// DBTITLE 1,Modify individual files ACLs
import com.pg.bigdata.octopufs.acl.AclManager
implicit val conf = spark.sparkContext.hadoopConfiguration

val acl = AclManager.FsPermission("user", "r--","ACCESS","jacek") //set read only permission for user jacek ("jacek" shoulld be replaced by object ID of the user from azure active directory)
//val acl = AclManager.FsPermission("group", "r--","ACCESS","securityGroupID") //set read only permission for group securityGroupID ("securityGroupID" shoulld be replaced by object ID of the group from azure active directory)
val paths = Array("abfss://container@adlsname.dfs.core.windows.net/j/IRI/asd_20200111_20200516.zip","abfss://container@adlsname.dfs.core.windows.net/j/IRI/spd_20200111_20200516.zip")
AclManager.modifyAcl(paths, acl)              

// COMMAND ----------

// DBTITLE 1,Remove all ACLs (leave just administrative ones)
import com.pg.bigdata.octopufs.acl.AclManager
implicit val conf = spark.sparkContext.hadoopConfiguration

//AclManager.clearFolderAcl("abfss://user@adlsname.dfs.core.windows.net/j/IRI") //recently renamed from clearFolderAcls //removes ACLs in a folder and all its descendants
AclManager.clearAcl(Array("abfss://container@adlsname.dfs.core.windows.net/j/IRI/asd_20200111_20200516.zip")) //recently renamed from clearAcls //removes ACLs from provided paths

// COMMAND ----------

// DBTITLE 1,Modify ACLs for all files and folders of a hive table
import com.pg.bigdata.octopufs.acl.AclManager
implicit val s = spark
val acl = AclManager.FsPermission("user", "r--","ACCESS","jacek") 
AclManager.modifyTableACLs("database_name", "table_name", acl)

// COMMAND ----------

// DBTITLE 1,Synchronize ACLs between two folder trees
import com.pg.bigdata.octopufs.acl.AclManager
implicit val conf = spark.sparkContext.hadoopConfiguration
AclManager.synchronizeAcls("abfss://container@adlsname.dfs.core.windows.net/j/IRI", "abfss://container@adlsname.dfs.core.windows.net/j/IRI2")

// COMMAND ----------

// DBTITLE 1,Apply parent folder ACLs to all files and subfolders
import com.pg.bigdata.octopufs.acl.AclManager
implicit val conf = spark.sparkContext.hadoopConfiguration
val parentFolder = "abfss://container@adlsname.dfs.core.windows.net/j/IRI"
AclManager.synchronizeAcls(parentFolder, parentFolder)
