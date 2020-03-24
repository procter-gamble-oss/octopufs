package com.pg.bigdata.utils.fs

import java.util.concurrent.{ExecutorService, Executors}

import com.pg.bigdata.utils.SafetyFuse
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

import com.pg.bigdata.utils.helpers.implicits._

object LocalExecution extends Serializable {

  def moveFolderContent(sourceFolderUri: String, targetFolderUri: String, keepSourceFolder: Boolean = false, numOfThreads: Int = 1000, timeoutMin: Int = 10)
                       (implicit conf: Configuration): Array[FSOperationResult] = {
    println("Moving folders content: " + sourceFolderUri + "  ==>>  " + targetFolderUri)
    implicit val srcFs: FileSystem = getFileSystem(conf,sourceFolderUri)
    val trgFs = getFileSystem(conf,targetFolderUri) //needed just to check if both fs are the same
    checkIfFsIsTheSame(srcFs, trgFs)

    if (!doesMoveLookSafe(srcFs,sourceFolderUri, targetFolderUri))
      return Array[FSOperationResult]()

    //delete target folder
    val transaction = new SafetyFuse(targetFolderUri)

    if (!transaction.isInProgress()) {
      transaction.startTransaction()

      if (srcFs.exists(new Path(targetFolderUri)))
        deleteFolder(targetFolderUri, true)
      else
        srcFs.mkdirs(new Path(targetFolderUri))
    }

    val sourceFileList = srcFs.listStatus(new Path(sourceFolderUri)).map(x => x.getPath.toString)
    val targetFileList = sourceFileList.map(_.replaceAll(sourceFolderUri, targetFolderUri))
    val paths = sourceFileList.zip(targetFileList).map(x => Paths(x._1, x._2))

    println("Files to be moved: (10 first only)")
    paths.slice(0, 10).foreach(println)

    val res = moveFiles(paths, numOfThreads)(conf)

    if (!keepSourceFolder)
      if (srcFs.delete(new Path(sourceFolderUri), true))
        println("WARNING: Folder " + sourceFolderUri + "could not be deleted and was left on storage device")
    res
  }

  def moveFiles(paths: Array[Paths], timeoutMins: Int = 10, attempt: Int = 0)
               (implicit conf: Configuration): Array[FSOperationResult] = {
    println("Starting moveFiles - attempt: " + attempt + ". Paths to be moved: " + paths.length)
    implicit val srcFs: FileSystem = getFileSystem(conf,paths.head.sourcePath)
    val trgFs = getFileSystem(conf,paths.head.targetPath) //needed just to check if both fs are the same
    checkIfFsIsTheSame(srcFs,trgFs)

    val res = paths.map(x => Future {
      print(".")
      FSOperationResult(x.sourcePath, srcFs.rename(new Path(x.sourcePath), new Path(x.targetPath)))
    }).map(x => Await.result(x, timeoutMins.minutes))

    println("Number of files moved properly: " + res.count(_.success))
    println("Files with errors: " + res.count(!_.success))

    val failed = res.filter(!_.success)
    val pathsOfFailed = paths.filter(x => failed.map(_.path).contains(x.sourcePath))
    val reallyFailed = removeFalseNegatives(pathsOfFailed)
    /*
    //checking if failed actually exist in the source (they were moved but reported error?)
    val stillInSource = pathsOfFailed.filter(x => fs.exists(new Path(x.sourcePath)))
    //checking if failed exist in the target (they were moved but reported error?)
    val failedExistingInTarget = pathsOfFailed.filter(y => fs.exists(new Path(y.targetPath)))
    //step 1: if file exist in both places compare modification dates. If target is < then source, throw exception (someone modified the target?)
    val inBothPlaces = pathsOfFailed.filter(x => stillInSource.contains(x)). //leave only those which are still in source
      filter(y => failedExistingInTarget.contains(y)) //this now gives all paths which are both in target and source
    inBothPlaces.filter(fs.listStatus()) */

    if (reallyFailed.isEmpty) res
    else if (reallyFailed.length == paths.length || attempt > 4)
      throw new Exception("Move of files did not succeed. Attempt: " + attempt + ". Please check why and here are some of them: \n" +
        reallyFailed.map(_.sourcePath).slice(0, 10).mkString("\n"))
    else {
      val pathsForReprocessing = reallyFailed
      res.filter(_.success) ++ moveFiles(pathsForReprocessing, timeoutMins, attempt + 1)(conf)
    }
  }

  def deletePaths(fs: FileSystem, paths: Array[String], timeoutMin: Int = 10, attempt: Int = 0): Array[FSOperationResult] = {

    val res = paths.map(x =>
      Future {
        FSOperationResult(x, fs.delete(new Path(x), true))
      }
    ).map(x => Await.result(x, timeoutMin.minutes))

    val failed = res.filter(!_.success)
    println("Number of paths deleted properly: " + res.count(_.success == true))
    println("Files with errors: " + res.count(!_.success))
    if (failed.isEmpty) res
    else if (failed.length == paths.length || attempt > 4)
      throw new Exception("Delete of some paths did not succeed - please check why and here are some of them: \n" + failed.map(_.path).slice(0, 10).mkString("\n"))
    else
      res.filter(_.success) ++ deletePaths(fs, paths.filter(x => failed.map(_.path).contains(x)), timeoutMin, attempt + 1)
  }

  def deleteFolder(folderUriPath: String, deleteContentOnly: Boolean = false, timeoutMin: Int = 10)(implicit confEx: Configuration): Unit = {
    val fs = getFileSystem(confEx, folderUriPath)
    if (deleteContentOnly) {
      val objectList = fs.listStatus(new Path(folderUriPath))
      val paths = objectList.map(folderUriPath + "/" + _.getPath.getName)

      println("absFolderUri: " + folderUriPath)
      println("Deleting paths count:" + paths.length)
      println("First 20 files for deletion:")
      paths.slice(0, 20).foreach(println)
      deletePaths(fs, paths, timeoutMin)
    }
    else if (!fs.delete(new Path(folderUriPath), true)) throw new Exception("Failed to remove " + folderUriPath)
  }

  def getFalseNegatives(paths: Array[Paths])(implicit fs: FileSystem): Array[Paths] = {
    paths.filter(x => !fs.exists(new Path(x.sourcePath)) && fs.exists(new Path(x.targetPath)))
  }

  def removeFalseNegatives(paths: Array[Paths])(implicit fs: FileSystem): Array[Paths] = {
    paths.diff(getFalseNegatives(paths))
  }


}