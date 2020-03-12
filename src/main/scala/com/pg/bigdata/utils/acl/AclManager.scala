package com.pg.bigdata.utils.acl

import java.util.concurrent.Executors

import com.pg.bigdata.utils.fs.{FSOperationResult, _}
import com.pg.bigdata.utils.helpers.ConfigSerDeser
import com.pg.bigdata.utils.metastore._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.permission._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._
import scala.collection.immutable.HashMap
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}
import scala.util.Try

object AclManager extends Serializable {

  def modifyTableACLs(db: String, tableName: String, newPermission: FsPermission, partitionCount: Int = 30)
                     (implicit spark: SparkSession, confEx: Configuration): Array[FSOperationResult] = {
    import collection.JavaConverters._

    val sdConf = new ConfigSerDeser(confEx)
    val loc = getTableLocation(db, tableName)
    val files = getListOfTableFiles(db, tableName).toList

    getFileSystem(confEx, loc).modifyAclEntries(new Path(getRelativePath(loc)), Seq(AclManager.getAclEntry(newPermission.getDefaultLevelPerm())).asJava)

    println(files.head)
    println("Files to process: " + files.length)
    modifyAcl(files, loc, partitionCount, sdConf, newPermission)

  }

  def modifyAcl(paths: List[String], parentFolderURI: String, partitionCount: Int, sdConf: ConfigSerDeser, newFsPermission: FsPermission)
               (implicit spark: SparkSession): Array[FSOperationResult] = {
    import scala.concurrent.ExecutionContext.Implicits.global
    spark.sparkContext.parallelize(paths, partitionCount).mapPartitions(part => {
      val conf = sdConf.get()
      val y = AclManager.getAclEntry(newFsPermission)
      val fs = getFileSystem(conf, parentFolderURI)
      val parentFolder = new Path(getRelativePath(parentFolderURI))
      if (fs.isDirectory(parentFolder))
        fs.modifyAclEntries(parentFolder, Seq(AclManager.getAclEntry(newFsPermission.getDefaultLevelPerm())).asJava)

      part.map(x => Future {
        Try({
          fs.modifyAclEntries(new Path(x), Seq(y).asJava)
          FSOperationResult(x, true)
        }).getOrElse(FSOperationResult(x, false))
      }).map(x => Await.result(x, 1.minute))
    }
    ).collect()
  }

  def getAclEntry(p: FsPermission): AclEntry = {
    val ptype = if (p.scope == p.USER) AclEntryType.USER
    else if (p.scope == p.GROUP) AclEntryType.GROUP
    else if (p.scope == p.OTHER) AclEntryType.OTHER
    else AclEntryType.MASK

    val perm = FsAction.getFsAction(p.permission)
    if (perm == null)
      throw new Exception("Provided permission " + p.permission + " is not valid for FsAction")
    val level = AclEntryScope.valueOf(p.level)
    val x = new AclEntry.Builder()
    val y = x.
      setType(ptype).
      setPermission(perm).
      setScope(level).
      setName(p.granteeObjectId).build()
    println(y.toString)
    y
  }

  def modifyFolderACLs(folderUri: String, newPermission: FsPermission, partitionCount: Int = 30, driverParallelism: Int = 1000)(implicit spark: SparkSession, confEx: Configuration): Array[FSOperationResult] = {
    implicit val pool = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(driverParallelism))
    val sdConf = new ConfigSerDeser(confEx)
    val fs = getFileSystem(confEx, folderUri)
    val elements = listRecursively(fs, new Path(folderUri))
    val folders = elements.filter(_.isDirectory).map(_.path)
    val files = elements.filter(!_.isDirectory).map(_.path)

    println(files(0))
    println("Files to process: " + files.length)
    println("Folders to process: " + folders.length)

    println("Changing file and folders ACCESS ACLs: " + files.length)
    val resAccess = modifyAcl(elements.map(_.path).toList, folderUri, partitionCount, sdConf, newPermission)
    println("Changing folders Default ACLs: " + folders.length)
    val resDefault = modifyAcl(folders.toList, folderUri, partitionCount, sdConf, newPermission.getDefaultLevelPerm())
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
  def synchronizeAcls(fs: FileSystem, sourceFolderRelativePath: String, targetFolderRelativePath: String, timeoutMin: Int = 10, numOfThreads: Int = 32, attempt: Int = 0): Unit = {
    val executor = Executors.newFixedThreadPool(numOfThreads)
    implicit val pool = ExecutionContext.fromExecutor(executor)

    println("Getting files from " + targetFolderRelativePath)
    val targetObjectList = listRecursively(fs, new Path(targetFolderRelativePath)).map(_.toRelativePath)
    println(targetObjectList.size.toString + " objects found in " + targetFolderRelativePath)
    val targetFolders = targetObjectList.filter(_.isDirectory)

    //getting entry for top level target folder
    val topAcl = getAclsForPaths(fs, Array(targetFolderRelativePath)).head._2
    println("Target folder ACL is: " + topAcl)

    println("Getting files from " + sourceFolderRelativePath)
    val sourceObjectList = listRecursively(fs, new Path(sourceFolderRelativePath)).map(_.toRelativePath)
    println(sourceObjectList.size.toString + " objects found in " + sourceFolderRelativePath)
    val sourceFiles = sourceObjectList.filter(!_.isDirectory)


    println(s"Getting ACLs for folders")
    val acls = getAclsForPaths(fs, targetFolders.map(_.path) :+ targetFolderRelativePath) //get acls for reporting folders
    val aclsHM = HashMap(acls: _*)

    println("Assigning ACLs on source folders")

    /** This function assigns ACL for the folders. If corresponding folder exists in target as it is in source, source folder gets target object's ACL assigned. If there is no corresponing folder, ACLs from parent folder are inherited
     *
     * @param sourceRootPath Source folder tree root
     * @param defaultAcl     Target root folder ACLs
     * @return list of paths and AclStatuses according to the following logic: if corresponding folder is found in target, then apply it's acl settings. Otherwise take that folder's parent ACLs
     */
    def findIdealAcl(sourceRootPath: String, defaultAcl: AclStatus): Array[AclSetting] = {
      val list = fs.listStatus(new Path(sourceRootPath)).filter(_.isDirectory)
      if (list.isEmpty) Array[AclSetting]()
      else {
        val currAcls = list.map(x => {
          val p = getRelativePath(x.getPath.toString)
          AclSetting(p, aclsHM.getOrElse(p.replace(sourceFolderRelativePath, targetFolderRelativePath), defaultAcl))
        })
        currAcls ++ currAcls.flatMap(z => findIdealAcl(z.path, z.aclStatus))
      }
    }

    println("Finding ACLs for folders...")
    val aclsOnSourceFolders = findIdealAcl(sourceFolderRelativePath, topAcl) :+ AclSetting(sourceFolderRelativePath, topAcl) //this returns source path and applied acl settings. THis will serve later to find parent folder's ACLs
    println("Acls assigned (not yet applied to these folders: (5 first only)")
    aclsOnSourceFolders.slice(0, 5).foreach(x => println(x.path + " - " + x.aclStatus.toString))
    println("Number of folder settings to be applied: " + aclsOnSourceFolders.length)

    def applyFolderSecurity(objects: Array[AclSetting], attempt: Int = 0): Array[FSOperationResult] = {
      val res = aclsOnSourceFolders.map(x => Future {
        val exec = Try(fs.setAcl(new Path(x.path), x.aclStatus.getEntries))
        if (exec.isFailure) println(x.path+ " ### " + x.aclStatus +"\n"+ exec.failed.get.getMessage)
        FSOperationResult(x.path, exec.isSuccess)
      }).map(x => Await.result(x, 10.minute))
      val failed = res.filter(!_.success)
      if (failed.isEmpty) res
      else if (failed.length == objects.length || attempt > 4)
        throw new Exception("Setting of ACLs did not succeed - please check why and here are some of them: \n" + failed.map(_.path).slice(0, 10).mkString("\n"))
      else
        res.filter(_.success) ++ applyFolderSecurity(objects.filter(x => failed.map(_.path).contains(x.path)), attempt + 1)
    }

    applyFolderSecurity(aclsOnSourceFolders)

    println("Create hashmap with folders' ACLs...")
    val aclsForFilesInFoldersHM = HashMap(aclsOnSourceFolders.map(x => (x.path, x.aclStatus)): _*)
    println("Assigning ACLs on files: (First 5 from the list)")
    sourceFiles.slice(0, 5).foreach(x => println(x.path))

    println("Number of files be modified (ACLs): " + sourceFiles.length)

    def applyFilesSecurity(objects: Array[FSElement], attempt: Int = 0): Array[FSOperationResult] = {
      val res =
        objects.map(x => Future {
          val parentFolder = new Path(x.path).getParent.toString
          val fileAcls = getAccessScopeAclFromDefault(aclsForFilesInFoldersHM.get(parentFolder).get)
          val exec = Try(fs.setAcl(new Path(x.path), fileAcls.asJava))
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
    applyFilesSecurity(sourceFiles)
    println("All done!!!...")
    executor.shutdown()
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
