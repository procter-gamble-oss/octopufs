package com.pg.bigdata.utils

import java.util.concurrent.Executors
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import scala.concurrent.duration._


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
      elements.map(x => FSElement(x.getPath.toString, x.isDirectory, x.getLen))
  }


  def getSizeInMB(path: String, driverParallelism: Int = 100, timeoutInMin: Int = 20)(implicit conf: Configuration): Double = {
    val fs = getFileSystem(conf, path)
    val pool = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(driverParallelism))
    val files = listRecursively(fs, new Path(path), timeoutInMin)(pool)
    val sizeInMb = files.map(_.byteSize).sum.toDouble / 1024 / 1024
    println("Size of " + path + " is " + (sizeInMb * 1000).round.toDouble / 1000 + " MB")
    sizeInMb
  }


  def checkIfFsIsTheSame(targetLocationUri: String, sourceFolderUri: String): Unit = {
    if (getFileSystemPrefix(targetLocationUri) + getContainerName(targetLocationUri) != getFileSystemPrefix(sourceFolderUri) + getContainerName(sourceFolderUri))
      throw new Exception("Cannot move files between 2 different filesystems. Use copy instead")
  }

  def doesMoveLookSafe(fs: FileSystem, sourceRelPath: String, targetRelPath: String): Boolean = {
    if (!fs.exists(new Path(sourceRelPath))) throw new Exception("Source folder " + sourceRelPath + " does not exist")
    val src = fs.listStatus(new Path(sourceRelPath))
    val trg = if (fs.exists(new Path(targetRelPath)))
      fs.listStatus(new Path(targetRelPath))
    else return true

    if (src.nonEmpty || (src.isEmpty && trg.isEmpty)) true
    else {
      println("Looks like your source folder " + sourceRelPath + " is empty, but your target folder " + targetRelPath +
        " is not. Skipping the move to avoid harmful folder move (assuming it is rerun)")
      false
    }
  }

   def copySingleFile(hadoopConf: Configuration, sourcePath: String, targetPath: String, sourceFileSystem: FileSystem, targetFileSystem: FileSystem,
                             overwrite: Boolean = true, deleteSource: Boolean = false): Boolean = {
    println(sourcePath + " => " + targetPath)
    val srcPath = new Path(sourcePath)
    val destPath = new Path(targetPath)
    org.apache.hadoop.fs.FileUtil.copy(sourceFileSystem, srcPath, targetFileSystem, destPath, deleteSource, overwrite, hadoopConf)
  }

}
