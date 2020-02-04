package com.pg.bigdata.utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.concurrent.{Await, ExecutionContextExecutor, Future}
import scala.concurrent.duration._

package object fs {
  var magicPrefix = ".dfs.core.windows.net"

  def getContainerName(uri: String): String = {
    if(uri.contains("@"))
      uri.substring(uri.indexOf("//")+2,uri.indexOf("@"))
    else
      "noContainerInPath"
  }

  def getRelativePath(uri: String): String = {
    if(magicPrefix=="") uri
    else if(!uri.contains(magicPrefix))
      throw new Exception("MagicPrefix not found")
    else
      uri.substring(uri.indexOf(magicPrefix)+magicPrefix.length)
  }

  def getFileSystem(hadoopConf: Configuration, absoluteTargetLocation: String): FileSystem = {
    //hadoopConf.set("fs.defaultFS", getFileSystemPrefix(absoluteTargetLocation))
    FileSystem.get(hadoopConf)
  }

  def getFileSystemPrefix(uri: String): String = {
    if(!uri.contains(magicPrefix))
      throw new Exception("MagicPrefix "+magicPrefix+" not found in "+uri)
    uri.substring(0,uri.indexOf(magicPrefix)+magicPrefix.length)
  }

  def listRecursively(fs: FileSystem, sourceFolder: Path, timeoutMin: Int=20)(implicit pool: ExecutionContextExecutor): Array[FSElement] = {
    val elements = fs.listStatus(sourceFolder)
    val folders = elements.filter(_.isDirectory)
    if(folders.isEmpty) elements.filter(!_.isDirectory).map(x => FSElement(x.getPath.toString, false))
    else folders.map(folder => Future{listRecursively(fs, folder.getPath)}(pool)).flatMap(x => Await.result(x, timeoutMin.minutes)) ++
      folders.map(x => FSElement(x.getPath.toString, true))
  }
}
