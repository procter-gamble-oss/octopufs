package com.pg.bigdata.utils.fs

import java.util.concurrent.Executors

import com.pg.bigdata.utils.Assistant.getTablesPathsList
import com.pg.bigdata.utils.Promotor
import com.pg.bigdata.utils.helpers.ConfigSerDeser
import com.pg.bigdata.utils.metastore.{filterPartitions, getListOfTableFiles, getTableLocation, refreshMetadata}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._

object LocalExecution extends Serializable {

  def moveFolderContent(sourceFolderUri: String, targetLocationUri: String, keepSourceFolder: Boolean = false, numOfThreads: Int = 192)
                       (implicit conf: Configuration): Array[FSOperationResult] = {
    println("Moving folders content: " + sourceFolderUri + "  ==>>  " + targetLocationUri)
    checkIfFsIsTheSame(sourceFolderUri, targetLocationUri)
    val fs = getFileSystem(conf, sourceFolderUri)

    if (!doesMoveLookSafe(fs, getRelativePath(sourceFolderUri), getRelativePath(targetLocationUri)))
      return Array()
    //delete target folder
    if (fs.exists(new Path(getRelativePath(targetLocationUri)))) //todo test if it works
      deleteAllChildObjectsLocal(targetLocationUri)
    else
      fs.mkdirs(new Path(targetLocationUri))

    val sourceFileList = fs.listStatus(new Path(sourceFolderUri)).map(x => x.getPath.toString)
    val targetFileList = sourceFileList.map(_.replaceAll(sourceFolderUri, targetLocationUri))
    val paths = sourceFileList.zip(targetFileList).map(x => Paths(x._1, x._2))

    val relativePaths = paths.map(x => Paths(getRelativePath(x.sourcePath), getRelativePath(x.targetPath)))
    println("ALL TO BE MOVED:")
    relativePaths.foreach(println)
    val res = moveFiles(relativePaths, sourceFolderUri, numOfThreads)(conf)
    if (res.exists(!_.success))
      throw new Exception("Move operation was not successful. There are " + res.count(!_.success) + " of objects which was not moved... " +
        "The list of first 100 which was not moved is below...\n" +
        res.map(_.path).slice(0, 99).mkString("\n"))

    if (!keepSourceFolder)
      if (fs.delete(new Path(getRelativePath(sourceFolderUri)), true))
        println("WARNING: Folder " + sourceFolderUri + "could not be deleted and was left on storage device")
    res.toArray
  }

  def moveFiles(relativePaths: Array[Paths], sourceFolderUri: String, numOfThreads: Int = 32, timeoutMins: Int = 10, attempt: Int = 0)
                    (implicit conf: Configuration): Array[FSOperationResult] = {
    println("Starting moveFiles. Paths to be moved: " + relativePaths.size)
    implicit val pool = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(numOfThreads))
    val fs = getFileSystem(conf, sourceFolderUri)
    val res = relativePaths.map(x => Future {
      print(".")
      FSOperationResult(x.sourcePath, fs.rename(new Path(x.sourcePath), new Path(x.targetPath)))
    }(pool)).map(x => Await.result(x, timeoutMins.minutes))

    println("Number of files moved properly: " + res.count(_.success))
    println("Files with errors: " + res.count(!_.success))
    val failed = res.filter(!_.success)

    if (failed.isEmpty) res
    else if (failed.length == relativePaths.length || attempt > 4)
      throw new Exception("Move of files did not succeed - please check why and here are some of them: \n" + failed.map(_.path).slice(0, 10).mkString("\n"))
    else {
      val failedPaths = relativePaths.map(_.sourcePath).filter(x => failed.map(_.path).contains(x))
      val pathsForReprocessing = relativePaths.filter(x => failedPaths.contains(x.sourcePath))
      res.filter(_.success) ++ moveFiles(pathsForReprocessing, sourceFolderUri, numOfThreads, timeoutMins, attempt + 1)(conf)
    }
  }

  def deletePaths(fs: FileSystem, paths: Array[String], timeoutMin: Int = 10, numOfThreads: Int = 32, attempt: Int = 0)
                 (implicit confEx: Configuration): Array[FSOperationResult] = {
    val exec = Executors.newFixedThreadPool(numOfThreads)
    implicit val pool = ExecutionContext.fromExecutor(exec)
    val res = paths.map( x =>
     Future {
        FSOperationResult(x, fs.delete(new Path(x), true))
      }
  ).map(x => Await.result(x, timeoutMin.minutes))
    exec.shutdown()
    val failed = res.filter(!_.success)
    println("Number of paths deleted properly: " + res.count(_.success == true))
    println("Files with errors: " + res.count(!_.success))
    if (failed.isEmpty) res
    else if (failed.length == paths.length || attempt > 4)
      throw new Exception("Delete of some paths did not succeed - please check why and here are some of them: \n" + failed.map(_.path).slice(0, 10).mkString("\n"))
    else
      res.filter(_.success) ++ deletePaths(fs, paths.filter(x => failed.contains(x)), timeoutMin, numOfThreads, attempt + 1)
  }

  def deleteAllChildObjectsLocal(folderAbsPath: String, timeoutMin: Int = 10, parallelism: Int = 32, attempt: Int = 0)(implicit confEx: Configuration): Unit = {
    val relPath = getRelativePath(folderAbsPath)
    val fs = getFileSystem(confEx, folderAbsPath)
    val objectList = fs.listStatus(new Path(relPath))
    val paths = objectList.map(relPath + "/" + _.getPath.getName)

    println("absFolderUri: " + folderAbsPath)
    println("deleting paths count:" + paths.size)
    println("First 20 files for deletion:")
    paths.slice(0, 20).foreach(println)
    deletePaths(fs,paths,timeoutMin,parallelism,attempt)
  }


}
