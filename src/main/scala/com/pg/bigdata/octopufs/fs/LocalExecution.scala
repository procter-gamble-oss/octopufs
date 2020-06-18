package com.pg.bigdata.octopufs.fs


import com.pg.bigdata.octopufs.SafetyFuse
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

import com.pg.bigdata.octopufs.helpers.implicits._

/**
 * Object contains functions which can be locally multithreaded (e.g. copy and move operations)
 */
object LocalExecution extends Serializable {

  /**
   * Moves all files from source folder and moves them to target folder. Operations are executed in parallel on local node. Operation is rerun safe.
   * @param sourceFolderUri - source folder full path, i.e. abfss://cont@cont.dfs....
   * @param targetFolderUri - target folder full path, i.e. abfss://cont@cont.dfs....
   * @param keepSourceFolder - controls if source folder should be deleted or not
   * @param conf - Hadoop Configuration [[org.apache.hadoop.conf.Configuration]]
   * @return - information about all processed paths together with information of operation was successful or not
   */
  def moveFolderContent(sourceFolderUri: String, targetFolderUri: String, keepSourceFolder: Boolean = false)
                       (implicit conf: Configuration): Array[FsOperationResult] = {
    println("Moving folders content: " + sourceFolderUri + "  ==>>  " + targetFolderUri)
    implicit val srcFs: FileSystem = getFileSystem(conf,sourceFolderUri)
    val trgFs = getFileSystem(conf,targetFolderUri) //needed just to check if both fs are the same
    checkIfFsIsTheSame(srcFs, trgFs)

    if (!doesMoveLookSafe(srcFs,sourceFolderUri, targetFolderUri))
      return Array[FsOperationResult]()

    //delete target foldet
    val transaction = new SafetyFuse("data/movefolderContent")

    if (!transaction.isInProgress()) {
      if (srcFs.exists(new Path(targetFolderUri)))
        deleteFolder(targetFolderUri, true)
      else
        srcFs.mkdirs(new Path(targetFolderUri))
      transaction.startTransaction()
    }

    val sourceFileList = srcFs.listStatus(new Path(sourceFolderUri)).map(x => x.getPath.toString)
    val targetFileList = sourceFileList.map(_.replaceAll(sourceFolderUri, targetFolderUri))
    val paths = sourceFileList.zip(targetFileList).map(x => Paths(x._1, x._2))

    println("Files to be moved: (10 first only)")
    paths.slice(0, 10).foreach(println)

    val res = movePaths(paths)(conf)

    if (!keepSourceFolder)
      if (srcFs.delete(new Path(sourceFolderUri), true))
        println("WARNING: Folder " + sourceFolderUri + "could not be deleted and was left on storage device")
    transaction.endTransaction()
    res
  }

  /**
   * Executes move operation on provided collection of [[com.pg.bigdata.octopufs.fs.Paths]]
   * @param paths - collection of files ([[com.pg.bigdata.octopufs.fs.Paths]]) to move
   * @param attempt - no not use
   * @param conf - Hadoop Configuration object [[org.apache.hadoop.conf.Configuration]]
   * @return - information about all processed paths together with information of operation was successful or not
   */
  def movePaths(paths: Array[Paths], attempt: Int = 0)(implicit conf: Configuration): Array[FsOperationResult] = {
    println("Starting moveFiles - attempt: " + attempt + ". Paths to be moved: " + paths.length)
    implicit val srcFs: FileSystem = getFileSystem(conf,paths.head.sourcePath)
    val trgFs = getFileSystem(conf,paths.head.targetPath) //needed just to check if both fs are the same
    checkIfFsIsTheSame(srcFs,trgFs)

    val res = paths.map(x => Future {
      print(".")
      FsOperationResult(x.sourcePath, srcFs.rename(new Path(x.sourcePath), new Path(x.targetPath)))
    }).map(x => Await.result(x, fsOperationTimeoutMinutes.minutes))

    println("Number of files moved properly: " + res.count(_.success))
    println("Files with errors: " + res.count(!_.success))

    val failed = res.filter(!_.success)
    val pathsOfFailed = paths.filter(x => failed.map(_.path).contains(x.sourcePath))
    val reallyFailed = removeFalseNegatives(pathsOfFailed)

    if (reallyFailed.isEmpty) res
    else if (reallyFailed.length == paths.length || attempt > 4)
      throw new Exception("Move of files did not succeed. Attempt: " + attempt + ". Please check why and here are some of them: \n" +
        reallyFailed.map(_.sourcePath).slice(0, 10).mkString("\n"))
    else {
      val pathsForReprocessing = reallyFailed
      res.filter(_.success) ++ movePaths(pathsForReprocessing, attempt + 1)(conf)
    }
  }

  /**
   * Deletes all files/folders provided in collection of absolute paths
   * @param paths - collection of paths, e.g. abfss://cont@storage.dfs....
   * @param attempt - do not use
   * @param conf - Hadoop Configuration object [[org.apache.hadoop.conf.Configuration]]
   * @return - information about all processed paths together with information of operation was successful or not
   */
  def deletePaths(paths: Array[String], attempt: Int = 0)(implicit conf: Configuration): Array[FsOperationResult] = {
    //todo add check if all paths are from the same fs
    val fs = getFileSystem(conf, paths(0))
    val res = paths.map(x =>
      Future {
        FsOperationResult(x, fs.delete(new Path(x), true))
      }
    ).map(x => Await.result(x, fsOperationTimeoutMinutes.minutes))

    val failed = res.filter(!_.success)
    println("Number of paths deleted properly: " + res.count(_.success == true))
    println("Files with errors: " + res.count(!_.success))
    if (failed.isEmpty) res
    else if (failed.length == paths.length || attempt > 4)
      throw new Exception("Delete of some paths did not succeed - please check why and here are some of them: \n" + failed.map(_.path).slice(0, 10).mkString("\n"))
    else
      res.filter(_.success) ++ deletePaths(paths.filter(x => failed.map(_.path).contains(x)), attempt + 1)
  }

  /**
   * Deletes a folder or only content of the folder, depending on deleteContentOnly parameter
   * @param folderUriPath - absoluite path of the folder, e.g abfss://cont@storage.dfs.......
   * @param deleteContentOnly - if true, folder will not be deleted, only the content
   * @param conf - Hadoop Configuration object [[org.apache.hadoop.conf.Configuration]]
   */
  def deleteFolder(folderUriPath: String, deleteContentOnly: Boolean = false)(implicit conf: Configuration): Unit = {
    val fs = getFileSystem(conf, folderUriPath)
    if (deleteContentOnly) {
      val objectList = fs.listStatus(new Path(folderUriPath))
      val paths = objectList.map(folderUriPath + "/" + _.getPath.getName)

      println("absFolderUri: " + folderUriPath)
      println("Deleting paths count:" + paths.length)
      println("First 20 files for deletion:")
      paths.slice(0, 20).foreach(println)
      deletePaths(paths)
    }
    else if (!fs.delete(new Path(folderUriPath), true)) throw new Exception("Failed to remove " + folderUriPath)
  }

  private def getFalseNegatives(paths: Array[Paths])(implicit fs: FileSystem): Array[Paths] = {
    paths.filter(x => !fs.exists(new Path(x.sourcePath)) && fs.exists(new Path(x.targetPath)))
  }

  private def removeFalseNegatives(paths: Array[Paths])(implicit fs: FileSystem): Array[Paths] = {
    paths.diff(getFalseNegatives(paths))
  }


}
