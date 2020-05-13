package com.pg.bigdata.utils


import com.pg.bigdata.utils.fs._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession

import com.pg.bigdata.utils.helpers.implicits._
class Delta(config: Configuration) {

  def synchronize(sourceUri: String, targetUri: String, copyParallelism: Int = -1, timeoutMin: Int = 10)
                 (implicit spark: SparkSession): Array[FsOperationResult] = {
    val (srcFs, trgFs, missingInTarget, onlyInTarget) = getDelta(sourceUri,targetUri, timeoutMin)

    LocalExecution.deletePaths(onlyInTarget.map(x => x.path), timeoutMin)(spark.sparkContext.hadoopConfiguration)
    val paths = missingInTarget.map(x => Paths(x.path, x.path.replace(sourceUri, targetUri)))

    DistributedExecution.copyFiles(paths, copyParallelism)(spark)
  }

  def getDelta(sourceUri: String, targetUri: String, timeoutMin: Int = 10) = {
    val srcFs = getFileSystem(config, sourceUri)
    val trgFs = getFileSystem(config, targetUri)
    val sourceFiles = listLevel(srcFs, Array(new Path(sourceUri)), timeoutMin).filter(!_.isDirectory)
    val targetFiles = listLevel(trgFs, Array(new Path(targetUri)), timeoutMin).filter(!_.isDirectory)
    val srcLocalPaths =  sourceFiles.map(x => FsElement(x.path.replaceAll(sourceUri,""),x.isDirectory,x.byteSize))
    val trgLocalPaths =  targetFiles.map(x => FsElement(x.path.replaceAll(targetUri,""),x.isDirectory,x.byteSize))
    val missingInTarget = srcLocalPaths.diff(trgLocalPaths).map(x => DeltaEntry(sourceUri+x.path,DeltaEntry.MISSING_IN_TARGET))
    val onlyInTarget = trgLocalPaths.diff(srcLocalPaths).map(x => DeltaEntry(targetUri+x.path,DeltaEntry.ONLY_IN_TARGET))
    (srcFs, trgFs, missingInTarget, onlyInTarget)
  }

}
