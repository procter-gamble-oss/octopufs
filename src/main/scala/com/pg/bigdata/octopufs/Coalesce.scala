package com.pg.bigdata.octopufs

import com.pg.bigdata.octopufs.fs
import com.pg.bigdata.octopufs.fs.{FsElement, Paths, getFileSystem}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.spark.sql.SparkSession

import java.util.concurrent.Executors
import scala.concurrent._


class Coalesce(numOfThreads: Int = 10)(implicit spark: SparkSession) {
  val exec = Executors.newFixedThreadPool(numOfThreads)
  implicit val executionContext: ExecutionContextExecutor = ExecutionContext.fromExecutor(exec)
  implicit val conf: Configuration = spark.sparkContext.hadoopConfiguration

  //if returns -1 then it means do not run coalesce
  private def figureOutNumberOfPartition(path: String, requestedFileSizeInMBytes: Long, tolerance: Double = 0.0)(implicit conf: Configuration): Int = {
    val requestedFileSizeInBytes: Long = requestedFileSizeInMBytes*1024*1024
    val fileDetails: Array[FileStatus] = getFileSystem(conf, path).listStatus(new Path(path)).filterNot(_.isDirectory)
    if(fileDetails.length<2) //if there is only one file - we don't need any coalesce
      -1
    else {
      val medianFileSize = fileDetails.map(_.getLen).sorted.apply(fileDetails.length/2)
      if(medianFileSize < (1-tolerance)*requestedFileSizeInBytes) //this means that we want to do coalesce
        (fileDetails.map(_.getLen).sum/requestedFileSizeInBytes).toInt.max(1)
      else
        -1 //do not do coalesce - files are big enough
    }
  }

  def doAutoCoalesce(source: String, requestedSizeOfFileMB: Long) = {
    implicit val conf: Configuration = spark.sparkContext.hadoopConfiguration
    val paths: Paths = prepareFolder(source)
    val df = spark.read.parquet(paths.sourcePath)
    val numOfFiles = figureOutNumberOfPartition(source,requestedSizeOfFileMB)(spark.sparkContext.hadoopConfiguration)
    println("Calculated coalesce count for " + source + " is " + numOfFiles)
    df.coalesce(numOfFiles).write.mode("overwrite").parquet(paths.targetPath)
    replaceFolder(paths.sourcePath, paths.targetPath)
  }

  def getLowestFoldersPaths(fs: FileSystem, topLevelPath: String): Array[String] = {
    //fs val fs = getFileSystem(spark.sparkContext.hadoopConfiguration, topLevelPath)
    val partitionFolders: Array[Path] = fs.listStatus(new Path(topLevelPath)).filter(_.isDirectory).map(_.getPath)
    //check if there is another level of folders - if yes, run recursive call
    if (partitionFolders.nonEmpty) {
      val subFolders = partitionFolders.flatMap(pf => fs.listStatus(pf).filter(_.isDirectory))
      if (subFolders.isEmpty) //if there are no subfolders, it means we are at the lowest level of folder structure
        partitionFolders.map(_.toString)
      else
        subFolders.flatMap(x => getLowestFoldersPaths(fs, x.getPath.toString))
    }
    else Array(topLevelPath)
    //no need for tail recursion as there will be only few recursive calls (usually 1-2)
  }

  private def prepareFolder(folderPath: String)(implicit conf: Configuration): Paths = {
    val fs = getFileSystem(conf, folderPath)
    //val path = new Path(folderPath)
    val targetPath = new Path(folderPath + "_temp")
    //val acl = fs.getAclStatus(path)
    println("Creating target folder")
    if(!fs.exists(targetPath) && !fs.mkdirs(targetPath))
      throw new Exception(s"Could not create the folder  $targetPath")
    //fs.setAcl(targetPath, acl.getEntries)
    println(s"Acls set up on ${targetPath.toString}")
    Paths(folderPath, targetPath.toString)
  }

  private def replaceFolder(folderToBeReplaced: String, replacement: String)(implicit conf: Configuration) = {
    val fs = getFileSystem(conf, folderToBeReplaced)
    fs.delete(new Path(folderToBeReplaced), true)
    if(!fs.rename(new Path(replacement), new Path(folderToBeReplaced)))
      throw new Exception(s"Rename operation $replacement -> $folderToBeReplaced failed")
  }


  def doPartitionCoalesce(path: String, requestedFileSizeMb: Int): Array[Future[Unit]] = {
    val folders: Array[String] = getLowestFoldersPaths(getFileSystem(spark.sparkContext.hadoopConfiguration, path),path)
    println("Got folders: "+folders.length)
    folders.map(partitionFolder =>
      Future{
        doAutoCoalesce(partitionFolder, requestedFileSizeMb)
      }
    )
  }

  def doItAll(paths: Array[String], requestedFileSizeMb: Int = 100): Unit = {
    paths.flatMap(doPartitionCoalesce(_, requestedFileSizeMb)).foreach(x => Await.result(x, duration.Duration.Inf))
  }
}
