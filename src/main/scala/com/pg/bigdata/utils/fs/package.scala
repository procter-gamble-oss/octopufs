package com.pg.bigdata.utils

import java.util.concurrent.Executors

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.permission.{AclEntry, AclEntryScope, AclStatus}
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.collection.JavaConverters._
import scala.collection.immutable.HashMap
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}

package object fs {
  var magicPrefix = ".dfs.core.windows.net"

  def getContainerName(uri: String): String = {
    if (uri.contains("@"))
      uri.substring(uri.indexOf("//") + 2, uri.indexOf("@"))
    else
      "noContainerInPath"
  }

  def getRelativePath(uri: String): String = {
    if (magicPrefix == "") uri
    else if (!uri.contains(magicPrefix))
      throw new Exception("MagicPrefix not found")
    else
      uri.substring(uri.indexOf(magicPrefix) + magicPrefix.length)
  }

  def getFileSystem(hadoopConf: Configuration, absoluteTargetLocation: String): FileSystem = {
    if (!getFileSystemPrefix(absoluteTargetLocation).startsWith("file")) //this is to avoid failures on local fs
      hadoopConf.set("fs.defaultFS", getFileSystemPrefix(absoluteTargetLocation))
    FileSystem.get(hadoopConf)
  }

  def getFileSystemPrefix(uri: String): String = {
    if (!uri.contains(magicPrefix))
      throw new Exception("MagicPrefix " + magicPrefix + " not found in " + uri)
    uri.substring(0, uri.indexOf(magicPrefix) + magicPrefix.length)
  }

  def listRecursively(fs: FileSystem, sourceFolder: Path, timeoutMin: Int = 20)(implicit pool: ExecutionContextExecutor): Array[FSElement] = {
    val elements = fs.listStatus(sourceFolder)
    val folders = elements.filter(_.isDirectory)
    if (folders.isEmpty) elements.filter(!_.isDirectory).map(x => FSElement(x.getPath.toString, false, x.getLen))
    else folders.map(folder => Future {
      listRecursively(fs, folder.getPath)
    }(pool)).flatMap(x => Await.result(x, timeoutMin.minutes)) ++
      folders.map(x => FSElement(x.getPath.toString, true, x.getLen))
  }

  def synchronizeAcls(fs: FileSystem, sourceFolder: String, targetFolder: String)(implicit pool: ExecutionContextExecutor): Unit = {
    println("Getting files from " + targetFolder)
    val targetObjectList = listRecursively(fs, new Path(targetFolder))
    println(targetObjectList.size.toString + " objects found in " + targetFolder)
    println("Getting files from " + sourceFolder)
    val sourceObjectList = listRecursively(fs, new Path(sourceFolder))
    println(sourceObjectList.size.toString + " objects found in " + sourceObjectList)
    val defaultTargetAcl = fs.getAclStatus(new Path(targetFolder))
    val targetFolders = targetObjectList.filter(_.isDirectory)
    println(s"Getting ACLs for folders")
    val acls = getAclsForPaths(fs, targetFolders.map(_.path) :+ targetFolder) //get acls for reporting folders
    val aclsHM = HashMap(acls: _*)
    val sourceFolders = sourceObjectList.filter(_.isDirectory)
    println("Assigning ACLs on source folders")

    /**
     *
     * @param sourceRootPath Source folder tree root
     * @param defaultAcl     Target root folder ACLs
     * @return list of paths and AclStatuses according to the following logic: if corresponding folder is found in target, then apply it's acl settings. Otherwise take that folder's parent ACLs
     */
    def findIdealAcl(sourceRootPath: String, defaultAcl: AclStatus): Seq[(String, AclStatus)] = {
      val list = fs.listStatus(new Path(sourceRootPath)).filter(_.isDirectory)
      if (list.isEmpty) Seq()
      else {
        val currAcls = list.map(x => (x.getPath.toString, aclsHM.getOrElse(x.getPath.toString.replace(sourceFolder, targetFolder), defaultAcl)))
        currAcls ++ currAcls.flatMap(z => findIdealAcl(z._1, z._2))
      }
    }

    val topAcl = getAclsForPaths(fs, Array(targetFolder)).head._2
    val aclsOnSourceFolders = findIdealAcl(sourceFolder, topAcl) :+ (sourceFolder, topAcl) //this returns source path and applied acl settings. THis will serve later to find parent folder's ACLs
    aclsOnSourceFolders.map(x => Future {
      fs.setAcl(new Path(x._1), x._2.getEntries)
    }).map(x => Await.result(x, 10.minute))

    println("Create hashmap with folders' ACLs...")
    val aclsForFilesInFoldersHM = HashMap(aclsOnSourceFolders: _*)
    println("Assigning ACLs on files")
    sourceObjectList.filter(!_.isDirectory).map(x => Future {
      val parentFolder = new Path(x.path).getParent.toString
      val fileAcls = getAccessScopeAclFromDefault(aclsForFilesInFoldersHM.get(parentFolder).get)
      fs.setAcl(new Path(x.path), fileAcls.asJava)
    }).map(x => Await.result(x, 10.minute))
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

  def getSizeInMB(path: String, driverParallelism: Int = 100, timeoutInMin: Int = 20)(implicit conf: Configuration): Double = {
    val fs = getFileSystem(conf, path)
    val pool = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(driverParallelism))
    val files = listRecursively(fs, new Path(path), timeoutInMin)(pool)
    val sizeInMb = files.map(_.byteSize).sum.toDouble / 1024 / 1024
    println("Size of " + path + " is " + (sizeInMb * 1000).round.toDouble / 1000 + " MB")
    sizeInMb
  }
}
