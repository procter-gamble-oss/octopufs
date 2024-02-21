package com.pg.bigdata.octopufs

import com.pg.bigdata.octopufs.helpers.implicits._

import java.net.URI
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}

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
    FileSystem.get(new URI(absoluteTargetLocation.replace(' ','_')), hadoopConf) //replace is needed in case path contains a space - and it doesn't matter to determine with FS (space cannot be in fs name)
  }

  /**
   * Gets all files and folders (recursively)
   * @param fs - FileSystem instance
   * @param folder - the foldeffe files for
   * @param dropFileDetail - If set to true, it will aggregate information about files to single element
   * @return - returns list of FsElement objects
   */

  def listLevel(fs: FileSystem, folder: Path, dropFileDetail: Boolean = false): Array[FsElement] = {
    def getFsElementList(fs: FileSystem, startingPoints: Array[Path],acc: Array[FsElement]): Array[FsElement] = {
      val fsElements = startingPoints.map(x => Future {
        fs.listStatus(x)
      }.map( elements => {
        if(dropFileDetail && elements.nonEmpty)
          elements.filter(_.isDirectory).map(FilesStatusToFsElement) :+ sumUpFiles(elements)
        else elements.map(FilesStatusToFsElement)
      })).flatMap(x => Await.result(x, fsOperationTimeoutMinutes.minutes))

      if (fsElements.filter(_.isDirectory).isEmpty) acc ++ fsElements
      else getFsElementList(fs, fsElements.filter(_.isDirectory).map(p => new Path(p.path)), acc ++ fsElements)
    }
    getFsElementList(fs,Array(folder), Array[FsElement]())

  }

  def list(absolutePath: String)(configuration: Configuration): Array[FsElement] = {
    val fs = getFileSystem(configuration, absolutePath)
    listLevel(fs, new Path(absolutePath),false)
  }

  private def FilesStatusToFsElement(x: FileStatus) = FsElement(x.getPath.toString, x.isDirectory, x.getLen)

  private def sumUpFiles(elements: Array[FileStatus]): FsElement = {
    val path = elements.head.getPath.getParent.toString + "/summed_up_files"
    FsElement(path,false, elements.filter(!_.isDirectory).map(_.getLen).sum)
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
  case class FsSizes(sizes: Array[FsElement], simplified: Boolean) {
    def getSizeOfPath(absolutePath: String): Double = {
      val list = sizes.filter(_.path.startsWith(absolutePath))
      val size = list.map(_.byteSize).sum.toDouble
      displayNumberOfFiles(absolutePath, list.length, simplified)
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

  def displayNumberOfFiles(path: String, numberOfFiles: Long, simplified: Boolean) = {
    println("Number of elements in " + path + " is " + numberOfFiles)
    if(simplified)
      println("File details have been skipped. Set skipFileDetails to false if you need individual file sizes")
  }

  /**
   * Gets size of all elements in the folder tree (assuming path is a folder).
   * @param path - absolute path
   * @param skipFileDetails - if set to true (default), it will not return individual files information, just summaries for the folders
   * @param conf - hadoop configuration
   * @return FsSizes which allow to browse through the elements of the filesystem without querying the storage account
   */
  def getSize(path: String, skipFileDetails: Boolean = true)(implicit conf: Configuration): FsSizes = {
    val fs = getFileSystem(conf, path)
    val files = listLevel(fs, new Path(path),skipFileDetails)
    displayNumberOfFiles(path, files.length, skipFileDetails)
    val size = files.map(_.byteSize).sum.toDouble
    displaySize(path, size)
    FsSizes(files, skipFileDetails)
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
