package com.pg.bigdata.utils

import java.util.concurrent.Executors

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

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

  def getSizeInMB(path: String, driverParallelism: Int = 100, timeoutInMin: Int = 20)(implicit conf: Configuration): Double = {
    val fs = FileSystem.get(conf)
    val pool = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(driverParallelism))
    val files = listRecursively(fs, new Path(path), timeoutInMin)(pool)
    val sizeInMb = files.map(_.byteSize).sum.toDouble / 1024 / 1024
    println("Size of " + path + " is " + (sizeInMb*1000).round.toDouble/1000 + " MB")
    sizeInMb
  }
}
