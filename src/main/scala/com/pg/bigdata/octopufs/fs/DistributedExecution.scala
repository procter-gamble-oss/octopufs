package com.pg.bigdata.octopufs.fs

import java.util.concurrent.Executors

import com.pg.bigdata.octopufs.fs
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import com.pg.bigdata.octopufs.helpers.implicits._
import org.apache.spark.{Partitioner, SerializableWritable}

import scala.concurrent.forkjoin.ForkJoinPool
import scala.concurrent.{Await, ExecutionContext, Future}


object DistributedExecution extends Serializable {

  /**
   * Function copies all files under specified source folder to target location. The copy is distributed to spark tasks. By default, each tasks gets exactly
   * one file for copy operation. This can be changed by providing different taskCount value. It can be useful especially when there is a lot of small files to be copied.
   * @param sourceFolderUri - absolute path to the source folder
   * @param targetLocationUri  - absolute path to the target folder
   * @param taskCount - number of spark taks to run. Keep default to have one file per task.
   * @param spark - spark session
   * @return - result of operation for each path
   */
  def copyFolder(sourceFolderUri: String, targetLocationUri: String, taskCount: Int = -1)(implicit spark: SparkSession): Array[FsOperationResult] = {
    implicit val conf = spark.sparkContext.hadoopConfiguration
    val srcFs = getFileSystem(conf, sourceFolderUri)
    val sourceFileList = listLevel(srcFs, Array(new Path(sourceFolderUri))).filter(!_.isDirectory).map(_.path) //filter is to avoid copying folders (folders will get created where copying files). Caveat: empty folders will not be copied
    val targetFileList = sourceFileList.map(_.replaceAll(sourceFolderUri, targetLocationUri)) //uri to work on differnt fikle systems
    val paths = sourceFileList.zip(targetFileList).map(x => Paths(x._1, x._2))
    println("First path is: " +paths.head)
    copyFiles(paths, taskCount)
  }

  /**
   * Copies all files in distributed manner. The copy is distributed to spark tasks. By default, each tasks gets exactly
   * * one file for copy operation. This can be changed by providing different taskCount value. It can be useful especially when there is a lot of small files to be copied.
   * @param paths - collection of files to be copied.
   * @param taskCount - number of spark taks to run. Keep default to have one file per task.
   * @param attempt - do not use
   * @param spark - SparkSession
   * @return - result of operation for each path
   */
  def copyFiles(paths: Seq[Paths], taskCount: Int = -1, attempt: Int = 0)
               (implicit spark: SparkSession): Array[FsOperationResult] = {

    val requestProcessed = spark.sparkContext.longAccumulator("CopyFilesProcessedCount")
    //val c = new org.apache.spark.util.SerializableConfiguration(spark.sparkContext.hadoopConfiguration)// ##change for runtime>6.4
    val c = new SerializableWritable[Configuration](spark.sparkContext.hadoopConfiguration)
    val confBroadcast = spark.sparkContext.broadcast(c)

    class PromotorPartitioner(override val numPartitions: Int) extends Partitioner {
      override def getPartition(key: Any): Int = key match {
        case (ind: Int) => ind % numPartitions
      }
    }

    val partCnt = if(taskCount == -1) paths.length else taskCount
    val rdd = spark.sparkContext.parallelize(paths.indices.map(i => (i,paths(i))), partCnt).keyBy(x => x._1).partitionBy(new PromotorPartitioner(partCnt)).values.values

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
      res.filter(_.success) ++ copyFiles(pathsForReprocessing, taskCount, attempt + 1)
    }
  }



}
