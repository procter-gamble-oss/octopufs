package com.pg.bigdata.octopufs

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.concurrent.duration._
import scala.concurrent.forkjoin.ForkJoinPool
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}


package object fs {

  //Assistant.buildInfo()

  def getFileSystem(hadoopConf: Configuration, absoluteTargetLocation: String): FileSystem = {
    FileSystem.get(new URI(absoluteTargetLocation), hadoopConf)
  }

  def listLevel(fs: FileSystem, folders: Array[Path], timeoutMin: Int = 20, level: Int = 0)(implicit pool: ExecutionContextExecutor): Array[FsElement] = {
    val elements = folders.map(x => Future {
      fs.listStatus(x)
    }).flatMap(x => Await.result(x, timeoutMin.minutes))
    val folderPaths = elements.filter(_.isDirectory).map(_.getPath)
    val fsElements = elements.map(x => FsElement(x.getPath.toString, x.isDirectory, x.getLen))
    if (folderPaths.isEmpty) fsElements
    else fsElements ++ listLevel(fs, folderPaths, timeoutMin, level + 1)
  }

  def toNiceSizeString(unitNames: Seq[String], size: Double): String = {
    if (size < 1024 || unitNames.tail.isEmpty) ((size * 100).round / 100.0).toString + " " + unitNames.head
    else toNiceSizeString(unitNames.tail, size / 1024)
  }

  case class FsSizes(sizes: Array[FsElement]) {
    def getSizeOfPath(absolutePath: String): Double = {
      val list = sizes.filter(_.path.startsWith(absolutePath))
      val size = list.map(_.byteSize).sum.toDouble
      displayNumberOfFiles(absolutePath, list.length)
      displaySize(absolutePath, size)
      size
    }
  }

  def displaySize(path: String, size: Double) = {
    val units = Seq("B", "KB", "MB", "GB", "TB")
    println("Size of " + path + " is " + toNiceSizeString(units, size))
  }

  def displayNumberOfFiles(path: String, numberOfFiles: Long) = println("Number of files in " + path + " is " + numberOfFiles)

  def getSize(path: String, driverParallelism: Int = 1000, timeoutInMin: Int = 20)(implicit conf: Configuration): FsSizes = {
    val fs = getFileSystem(conf, path)
    val exec = new ForkJoinPool(driverParallelism)
    val pool = ExecutionContext.fromExecutor(exec)
    val files = listLevel(fs, Array(new Path(path)), timeoutInMin)(pool)
    displayNumberOfFiles(path, files.length)
    val size = files.map(_.byteSize).sum.toDouble
    displaySize(path, size)
    FsSizes(files)
  }

  def checkIfFsIsTheSame(srcFs: FileSystem, trgFs: FileSystem): Unit = {
    if (srcFs.getUri != trgFs.getUri)
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
