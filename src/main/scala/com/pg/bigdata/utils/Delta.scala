package com.pg.bigdata.utils
import com.pg.bigdata.utils.fs._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs._
import org.apache.spark.sql.SparkSession
class Delta(config: Configuration) {

  def synchronize(sourceUri: String, targetUri: String, copyParallelism: Int = 200, timeoutMin: Int = 10, deleteParallelism: Int = 1000)
                 (implicit spark: SparkSession): Array[FSOperationResult] = {
    val (srcFs, trgFs, missingInTarget, onlyInTarget) = getDelta(sourceUri,targetUri, timeoutMin)

    LocalExecution.deletePaths(trgFs, onlyInTarget.map(x => x.path), timeoutMin,deleteParallelism)
    val paths = missingInTarget.map(x => Paths(x.path, x.path.replace(sourceUri, targetUri)))

    DistributedExecution.copyFiles(sourceUri, targetUri, paths, copyParallelism)(spark, config)
  }

  def getDelta(sourceUri: String, targetUri: String, timeoutMin: Int = 10) = {
    val srcFs = getFileSystem(config, sourceUri)
    val trgFs = getFileSystem(config, targetUri)
    val envSrc = LocalExecution.getExecutorAndPool(1000)
    val envTrg = LocalExecution.getExecutorAndPool(1000)
    val sourceFiles = listRecursively(srcFs, new Path(sourceUri), timeoutMin)(envSrc.pool).filter(!_.isDirectory)
    val targetFiles = listRecursively(trgFs, new Path(targetUri), timeoutMin)(envTrg.pool).filter(!_.isDirectory)
    val srcLocalPaths =  sourceFiles.map(x => FSElement(x.path.replaceAll(sourceUri,""),x.isDirectory,x.byteSize))
    val trgLocalPaths =  targetFiles.map(x => FSElement(x.path.replaceAll(targetUri,""),x.isDirectory,x.byteSize))
    val missingInTarget = srcLocalPaths.diff(trgLocalPaths).map(x => DeltaEntry(sourceUri+x.path,DeltaEntry.MISSING_IN_TARGET))
    val onlyInTarget = trgLocalPaths.diff(srcLocalPaths).map(x => DeltaEntry(targetUri+x.path,DeltaEntry.ONLY_IN_TARGET))
    envSrc.executor.shutdown()
    envTrg.executor.shutdown()
    (srcFs, trgFs, missingInTarget, onlyInTarget)
  }

}
