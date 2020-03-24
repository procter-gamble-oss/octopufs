package com.pg.bigdata.utils.acl

import java.util.concurrent.Executors

import com.pg.bigdata.utils.fs.{FSOperationResult, _}
import com.pg.bigdata.utils.metastore._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.permission._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._
import scala.collection.immutable.HashMap
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.Try

import com.pg.bigdata.utils.helpers.implicits._

object AclManager extends Serializable {

  def modifyTableACLs(db: String, tableName: String, newPermission: FsPermission, partitionCount: Int = 1000)
                     (implicit spark: SparkSession, conf: Configuration): Array[FSOperationResult] = {
    import collection.JavaConverters._

    val loc = getTableLocation(db, tableName)
    val files = getListOfTableFiles(db, tableName)

    getFileSystem(conf, loc).modifyAclEntries(new Path(loc), Seq(AclManager.getAclEntry(newPermission.getDefaultLevelPerm())).asJava)

    println(files.head)
    println("Files to process: " + files.length)
    modifyAcl(files, newPermission, partitionCount)
  }

  //todo add remove ACL
  //assumes the same fs for all
  def modifyAcl(paths: Array[String], newFsPermission: FsPermission, timeoutMin: Int = 10, numOfThreads: Int = 1000, attempt: Int = 0) //change to local
               (implicit conf: Configuration): Array[FSOperationResult] = {
    println("Settign ACLs - attempt " + attempt)

    val y = AclManager.getAclEntry(newFsPermission)
    val fs = getFileSystem(conf, paths.head)


    val res = paths.map(x => Future {
      Try({
        fs.modifyAclEntries(new Path(x), Seq(y).asJava)
        FSOperationResult(x, true)
      }).getOrElse(FSOperationResult(x, false))
    }).map(x => Await.result(x, timeoutMin.minute))
    val failed = res.filter(!_.success).filter(x => fs.exists(new Path(x.path))).map(_.path)
    if (failed.isEmpty) res
    else if (failed.length == paths.length || attempt > 4) throw new Exception("Some paths failed - showing 10 of them " + failed.slice(0, 10).mkString("\n"))
    else modifyAcl(failed, newFsPermission, timeoutMin, numOfThreads, attempt + 1)
  }


  def getAclEntry(p: FsPermission): AclEntry = {
    val ptype = if (p.scope == p.USER) AclEntryType.USER
    else if (p.scope == p.GROUP) AclEntryType.GROUP
    else if (p.scope == p.OTHER) AclEntryType.OTHER
    else AclEntryType.MASK

    val perm = FsAction.getFsAction(p.permission)
    if (perm == null)
      throw new Exception("Provided permission " + p.permission + " is not valid for FsAction")
    val level = if (p.level == "ACCESS") AclEntryScope.ACCESS else AclEntryScope.DEFAULT
    val x = new AclEntry.Builder()
    val y = x.
      setType(ptype).
      setPermission(perm).
      setScope(level).
      setName(p.granteeObjectId).build()
    println("ACL entry: " + y.toString)
    y
  }

  def modifyFolderACLs(folderUri: String, newPermission: FsPermission, partitionCount: Int = 30, parallelism: Int = 1000)(implicit spark: SparkSession, confEx: Configuration): Array[FSOperationResult] = {

    val fs = getFileSystem(confEx, folderUri)
    val elements = listLevel(fs, Array(new Path(folderUri)))
    val folders = elements.filter(_.isDirectory).map(_.path)
    val files = elements.filter(!_.isDirectory).map(_.path)

    println(files(0))
    println("Files to process: " + files.length)
    println("Folders to process: " + folders.length)

    println("Changing file and folders ACCESS ACLs: " + files.length)
    val resAccess = modifyAcl(elements.map(_.path), newPermission)
    println("Changing folders Default ACLs: " + folders.length)
    val resDefault = modifyAcl(folders, newPermission.getDefaultLevelPerm())
    resAccess.union(resDefault)
  }

  def getAclEntries(path: String)(implicit configuration: Configuration): Seq[AclEntry] = {
    val fs = getFileSystem(configuration, path)
    fs.getAclStatus(new Path(path)).getEntries.asScala.toSeq
  }

  def resetAclEntries(pathUri: String, acls: Seq[AclEntry])(implicit configuration: Configuration) = {
    val path = new Path(pathUri)
    val fs = getFileSystem(configuration, pathUri)
    println("Removing ACLs on " + pathUri + " and setting new entries")
    acls.foreach(println)
    fs.setAcl(path, acls.asJava)
  }

  case class FsPermission(scope: String, permission: String, level: String, granteeObjectId: String) {
    val USER: String = "user"
    val GROUP: String = "group"
    val OTHER: String = "other"
    val MASK: String = "mask"

    def getDefaultLevelPerm(): AclManager.FsPermission = FsPermission(scope, permission, "DEFAULT", granteeObjectId)
  }


  //TARGET folder is something which function is taking ACLs from and apply them to SOURCE folder
  def synchronizeAcls(uriOfFolderToApplyAclsTo: String, uriOfFolderToTakeAclsFrom: String, timeoutMin: Int = 10, numOfThreads: Int = 32, attempt: Int = 0)
                     (implicit conf: Configuration): Unit = {
    //path.isAbsoluteAndSchemeAuthorityNull
    val targetFs = getFileSystem(conf, uriOfFolderToApplyAclsTo)
    val sourceFs = getFileSystem(conf, uriOfFolderToTakeAclsFrom)

    println("Getting files from " + uriOfFolderToTakeAclsFrom)
    val sourceObjectList = listLevel(sourceFs, Array(new Path(uriOfFolderToTakeAclsFrom)))
    println(sourceObjectList.length.toString + " objects found in " + uriOfFolderToTakeAclsFrom)
    val nSourceFolders = sourceObjectList.filter(_.isDirectory)

    //getting entry for top level target folder
    val topAcl = getAclsForPaths(sourceFs, Array(uriOfFolderToTakeAclsFrom)).head._2
    println("Target folder ACL is: " + topAcl)

    println("Getting files from " + uriOfFolderToApplyAclsTo)
    val targetObjectList = listLevel(targetFs, Array(new Path(uriOfFolderToApplyAclsTo))) :+ FSElement(uriOfFolderToApplyAclsTo, true, 0) //adding top level folder
    println(targetObjectList.length.toString + " objects found in " + uriOfFolderToApplyAclsTo)
    val nTargetFiles = targetObjectList.filter(!_.isDirectory)


    println(s"Getting ACLs for folders")
    val acls = getAclsForPaths(sourceFs, nSourceFolders.map(_.path) :+ uriOfFolderToTakeAclsFrom) //get acls for reporting folders
    val aclsHM = HashMap(acls: _*)

    println("Assigning ACLs on source folders")

    /** This function assigns ACL for the folders. If corresponding folder exists in target as it is in source, source folder gets target object's ACL assigned. If there is no corresponing folder, ACLs from parent folder are inherited
     *
     * @param rootUriPath Folder tree root to apply acls on
     * @param defaultAcl  Root folder ACLs
     * @return list of paths and AclStatuses according to the following logic: if corresponding folder is found in target, then apply it's acl settings. Otherwise take that folder's parent ACLs
     */
    def findIdealAcl(rootUriPath: String, defaultAcl: AclStatus): Array[AclSetting] = {
      val list = targetFs.listStatus(new Path(rootUriPath)).filter(_.isDirectory)
      if (list.isEmpty) Array[AclSetting]()
      else {
        val currAcls = list.map(x => {
          val p = x.getPath.toString
          AclSetting(p, aclsHM.getOrElse(p.replace(uriOfFolderToApplyAclsTo, uriOfFolderToTakeAclsFrom), defaultAcl))
        })
        currAcls ++ currAcls.flatMap(z => findIdealAcl(z.path, z.aclStatus))
      }
    }

    println("Finding ACLs for folders...")
    val aclsOnTargetFolders = (findIdealAcl(uriOfFolderToApplyAclsTo, topAcl) :+ AclSetting(uriOfFolderToApplyAclsTo, topAcl)).sortBy(x => x.path.length) //this returns source path and applied acl settings. THis will serve later to find parent folder's ACLs
    //debug

    println("Acls assigned (not yet applied to these folders: (5 first only)")
    aclsOnTargetFolders.slice(0, 5).foreach(x => println(x.path + " - " + x.aclStatus.toString))
    println("Number of folder settings to be applied: " + aclsOnTargetFolders.length)

    def applyFolderSecurity(objects: Array[AclSetting], attempt: Int = 0): Array[FSOperationResult] = {
      val res = aclsOnTargetFolders.map(x => Future {
        targetFs.removeAcl(new Path(x.path))
        val exec = Try(targetFs.modifyAclEntries(new Path(x.path), x.aclStatus.getEntries))
        if (exec.isFailure) println(x.path + " ### " + x.aclStatus + "\n" + exec.failed.get.getMessage)
        FSOperationResult(x.path, exec.isSuccess)
      }).map(x => Await.result(x, 10.minute))
      val failed = res.filter(!_.success)
      if (failed.isEmpty) res
      else if (failed.length == objects.length || attempt > 4)
        throw new Exception("Setting of ACLs did not succeed - please check why and here are some of them: \n" + failed.map(_.path).slice(0, 10).mkString("\n"))
      else
        res.filter(_.success) ++ applyFolderSecurity(objects.filter(x => failed.map(_.path).contains(x.path)), attempt + 1)
    }

    applyFolderSecurity(aclsOnTargetFolders)

    println("Create hashmap with folders' ACLs...")
    val aclsForFilesInFoldersHM = HashMap(aclsOnTargetFolders.map(x => (x.path, x.aclStatus)): _*)

    aclsForFilesInFoldersHM.toList.slice(0, 6).foreach(x => println(x._1 + x._2))

    println("Assigning ACLs on files: (First 5 from the list)")
    nTargetFiles.slice(0, 5).foreach(x => println(x.path))

    println("Number of files be modified (ACLs): " + nTargetFiles.length)

    def applyFilesSecurity(objects: Array[FSElement], attempt: Int = 0): Array[FSOperationResult] = {
      val res =
        objects.map(x => Future {
          val parentFolder = new Path(x.path).getParent.toString
          println("parent folder: " + parentFolder)
          val fileAcls = getAccessScopeAclFromDefault(aclsForFilesInFoldersHM(parentFolder))
          val exec = Try(targetFs.setAcl(new Path(x.path), fileAcls.asJava))
          if (exec.isFailure) println(exec.failed.get.getMessage)
          FSOperationResult(x.path, exec.isSuccess)
        }).map(x => Await.result(x, 10.minute))
      val failed = res.filter(!_.success)
      if (failed.isEmpty) res
      else if (failed.length == objects.length || attempt > 4)
        throw new Exception("Setting of ACLs on files did not succeed - please check why and here are some of them: \n" + failed.map(_.path).slice(0, 10).mkString("\n"))
      else
        res.filter(_.success) ++ applyFilesSecurity(objects.filter(x => failed.map(_.path).contains(x.path)), attempt + 1)
    }

    applyFilesSecurity(nTargetFiles)
    println("All done!!!...")
  }

  def getAclsForPaths(fs: FileSystem, paths: Array[String]): Array[(String, AclStatus)] = {
    paths.map(x => {
      val p = new Path(x)
      (x, fs.getAclStatus(p))
    })
  }

  def getAccessScopeAclFromDefault(aclStatus: AclStatus): Seq[AclEntry] = {
    aclStatus.getEntries.asScala.filter(_.getScope == AclEntryScope.DEFAULT).map(x =>
      new AclEntry.Builder().setName(x.getName).setPermission(x.getPermission).setType(x.getType).
        setScope(AclEntryScope.ACCESS).build()
    )
  }
}
