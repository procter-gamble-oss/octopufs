package com.pg.bigdata.utils.fs

import java.util.concurrent.Executors

import com.pg.bigdata.utils.fs
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import com.pg.bigdata.utils.helpers.implicits._
import org.apache.spark.{Partitioner, SerializableWritable}

import scala.concurrent.forkjoin.ForkJoinPool
import scala.concurrent.{Await, ExecutionContext, Future}


object DistributedExecution extends Serializable {


  def copyFolder(sourceFolderUri: String, targetLocationUri: String, partitionCount: Int = -1)(implicit spark: SparkSession): Array[FsOperationResult] = {
    implicit val conf = spark.sparkContext.hadoopConfiguration
    val srcFs = getFileSystem(conf, sourceFolderUri)
    val sourceFileList = listLevel(srcFs, Array(new Path(sourceFolderUri))).filter(!_.isDirectory).map(_.path) //filter is to avoid copying folders (folders will get created where copying files). Caveat: empty folders will not be copied
    val targetFileList = sourceFileList.map(_.replaceAll(sourceFolderUri, targetLocationUri)) //uri to work on differnt fikle systems
    val paths = sourceFileList.zip(targetFileList).map(x => Paths(x._1, x._2))
    println("First path is: " +paths.head)
    copyFiles(paths, partitionCount)
  }

  def copyFiles(paths: Seq[Paths], partitionCount: Int = -1, attempt: Int = 0)
               (implicit spark: SparkSession): Array[FsOperationResult] = {

    val requestProcessed = spark.sparkContext.longAccumulator("CopyFilesProcessedCount")
    val confBroadcast = spark.sparkContext.broadcast(new SerializableWritable(spark.sparkContext.hadoopConfiguration))

    class PromotorPartitioner(override val numPartitions: Int) extends Partitioner {
      override def getPartition(key: Any): Int = key match {
        case (ind: Int) => ind % numPartitions
      }
    }

    val partCnt = if(partitionCount == -1) paths.length else partitionCount
    val rdd = spark.sparkContext.parallelize(paths.indices.map(i => (i,paths(i))), partCnt).keyBy(x => x._1).partitionBy(new PromotorPartitioner(partCnt)).values.values //todo sprawdz czy potreba jest ilosc partycji w parallelize
    //val res = spark.sparkContext.parallelize(paths, partitionCount)
    val res = rdd.mapPartitions(x => {
      //val conf = confsd.get()
      val conf: Configuration = confBroadcast.value.value
      val srcFs = getFileSystem(conf, paths.head.sourcePath)
      val trgFs = getFileSystem(conf, paths.head.targetPath)
      x.map(paths => {
        requestProcessed.add(1)
        FsOperationResult(paths.sourcePath, fs.copySingleFile(conf, paths.sourcePath, paths.targetPath, srcFs, trgFs))
      })
    }).collect()
    val failed = res.filter(!_.success)
    println("Number of files copied properly: " + res.count(_.success))
    println("Files with errors: " + failed.length)
    if (failed.isEmpty) res
    else if (failed.length == paths.length || attempt > 4)
      throw new Exception("Copy of files did not succeed - please check why and here are some of them: \n" + failed.map(_.path).slice(0, 10).mkString("\n"))
    else {
      val failedPaths = paths.map(_.sourcePath).filter(x => failed.map(_.path).contains(x))
      val pathsForReprocessing = paths.filter(x => failedPaths.contains(x.sourcePath))
      println("Reprocessing " + failedPaths.length + " of failed paths...")
      res.filter(_.success) ++ copyFiles(pathsForReprocessing, partitionCount, attempt + 1)
    }
  }
/*
  private def moveFiles(relativePaths: Seq[Paths], sourceFolderUri: String, partitionCount: Int = 32, attempt: Int = 0)
                       (implicit spark: SparkSession, confEx: Configuration): Array[FSOperationResult] = {
    println("Starting moveFiles. Paths to be moved: " + relativePaths.size)

    val requestProcessed = spark.sparkContext.longAccumulator("MoveFilesProcessedCount")
    val sdConf = new ConfigSerDeser(confEx)
    val res = spark.sparkContext.parallelize(relativePaths, partitionCount).mapPartitions(x => {
      val conf = sdConf.get()
      val srcFs = getFileSystem(conf, sourceFolderUri) //move can be done only within single fs, which makes sense :)
      x.map(paths => {
        requestProcessed.add(1)
        println("Executor paths: " + paths)
        Future(paths, srcFs.rename(new Path(paths.sourcePath), new Path(paths.targetPath))) //todo this fails if folder structure for the file does not exist
      })
    }).map(x => Await.result(x, 120.seconds)).map(x => FSOperationResult(x._1.sourcePath, x._2)).collect()
    println("Number of files moved properly: " + res.count(_.success))
    println("Files with errors: " + res.count(!_.success))
    val failed = res.filter(!_.success)

    if (failed.isEmpty) res
    else if (failed.length == relativePaths.length || attempt > 4)
      throw new Exception("Move of files did not succeed - please check why and here are some of them: \n" + failed.map(_.path).slice(0, 10).mkString("\n"))
    else {
      val failedPaths = relativePaths.map(_.sourcePath).filter(x => failed.map(_.path).contains(x))
      val pathsForReprocessing = relativePaths.filter(x => failedPaths.contains(x.sourcePath))
      println("Reprocessing " + failedPaths.length + " of failed paths...")
      res.filter(_.success) ++ moveFiles(pathsForReprocessing, sourceFolderUri, partitionCount, attempt + 1)
    }
  }

 */


}
