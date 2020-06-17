package com.pg.bigdata.octopufs

import com.pg.bigdata.octopufs.helpers.implicits._
import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.concurrent.duration._
import scala.concurrent.forkjoin.ForkJoinPool
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}


package object fs {

  //Assistant.buildInfo()
  /**
   * Gets Hadoop FileSystem object for tha path
   * @param hadoopConf - Hadoop Configuration
   * @param absoluteTargetLocation - path to get FileSystem for
   * @return FileSystem object
   */
  def getFileSystem(hadoopConf: Configuration, absoluteTargetLocation: String): FileSystem = {
    FileSystem.get(new URI(absoluteTargetLocation), hadoopConf)
  }

  /**
   * Gets all files and folders (recursively)
   * @param fs - FileSystem instance
   * @param folders - list of folders to get the files for. Usually one-element array is used as a starting point.
   * @param level - do not use
   * @return - returns list of FsElement objects
   */
  def listLevel(fs: FileSystem, folders: Array[Path], level: Int = 0): Array[FsElement] = {
    val elements = folders.map(x => Future {
      fs.listStatus(x)
    }).flatMap(x => Await.result(x, fsOperationTimeoutMinutes.minutes))
    val folderPaths = elements.filter(_.isDirectory).map(_.getPath)
    val fsElements = elements.map(x => FsElement(x.getPath.toString, x.isDirectory, x.getLen))
    if (folderPaths.isEmpty) fsElements
    else fsElements ++ listLevel(fs, folderPaths, level + 1)
  }

  /**
   * Formats size of the files nicely
   * @param unitNames Seq("B", "KB", "MB", "GB", "TB")
   * @param size actual size in bytes
   * @return nice size string
   */
  private def toNiceSizeString(unitNames: Seq[String], size: Double): String = {
    if (size < 1024 || unitNames.tail.isEmpty) ((size * 100).round / 100.0).toString + " " + unitNames.head
    else toNiceSizeString(unitNames.tail, size / 1024)
  }

  /**
   * Simple class to allow checking folder size without repeated querying of storage account
   * @param sizes - array of FsElement objects from listLevel function (for example)
   */
  case class FsSizes(sizes: Array[FsElement]) {
    def getSizeOfPath(absolutePath: String): Double = {
      val list = sizes.filter(_.path.startsWith(absolutePath))
      val size = list.map(_.byteSize).sum.toDouble
      displayNumberOfFiles(absolutePath, list.length)
      displaySize(absolutePath, size)
      size
    }
  }

  /**
   * Nicely prints out size of path
   * @param path
   * @param size - size in Bytes
   */
  def displaySize(path: String, size: Double) = {
    val units = Seq("B", "KB", "MB", "GB", "TB")
    println("Size of " + path + " is " + toNiceSizeString(units, size))
  }

  def displayNumberOfFiles(path: String, numberOfFiles: Long) = println("Number of files in " + path + " is " + numberOfFiles)

  /**
   * Gets size of all elements in the folder tree (assuming path is a folder).
   * @param path - absolute path
   * @param conf - hadoop configuration
   * @return FsSizes which allow to browse through the elements of the filesystem without querying the storage account
   */
  def getSize(path: String)(implicit conf: Configuration): FsSizes = {
    val fs = getFileSystem(conf, path)
    val files = listLevel(fs, Array(new Path(path)))
    displayNumberOfFiles(path, files.length)
    val size = files.map(_.byteSize).sum.toDouble
    displaySize(path, size)
    FsSizes(files)
  }

  /**
   * Checks is profided filesystem objects represent the same filesystem
   * @param srcFs
   * @param trgFs
   */
  def checkIfFsIsTheSame(srcFs: FileSystem, trgFs: FileSystem): Unit = {
    if (srcFs.getUri != trgFs.getUri)
      throw new Exception("Cannot move files between 2 different filesystems. Use copy instead")
  }

  /**
   * This function attempts to protect the user from moving empty folder to location, which contains files. The assumption is that this is user mistake
   * as it will effectively perform delete operation on the target folder.
   * @param fs - Hadoop FileSystem instance
   * @param sourceRelPath - source folder path
   * @param targetRelPath - target folder path
   * @return Returns true if move looks like safe
   */
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

  /**
   * Copies single file using FileUtil.copy operation. Prints out information on std output.
   * @param hadoopConf
   * @param sourcePath
   * @param targetPath
   * @param sourceFileSystem
   * @param targetFileSystem
   * @param overwrite
   * @param deleteSource
   * @return true if successful
   */
  def copySingleFile(hadoopConf: Configuration, sourcePath: String, targetPath: String, sourceFileSystem: FileSystem, targetFileSystem: FileSystem,
                     overwrite: Boolean = true, deleteSource: Boolean = false): Boolean = {
    println(sourcePath + " => " + targetPath)
    val srcPath = new Path(sourcePath)
    val destPath = new Path(targetPath)
    org.apache.hadoop.fs.FileUtil.copy(sourceFileSystem, srcPath, targetFileSystem, destPath, deleteSource, overwrite, hadoopConf)
  }

}
