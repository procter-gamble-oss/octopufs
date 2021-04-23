package com.pg.bigdata.octopufs


import com.pg.bigdata.octopufs.fs._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession

import com.pg.bigdata.octopufs.helpers.implicits._

/**
 * Object which helps to determine change in file tree and synchronize one with another, if needed
 * @param config
 */
class Delta(config: Configuration) {
  /**
   * Executes synchronization of two folders. First it detects the difference, then deletes paths from target folder, which do not have corresponding
   * files in source folder. Then it uses distributed copy to copy new files to the target folder.
   * @param sourceUri - folder which function will copy from
   * @param targetUri - folder which function will alignt to match source folder
   * @param taskCount - task count of spark copy operation. Keep it default (-1) to have best distribution - one file per park, unless you have a lot of small files)
   * @param spark
   * @return - list of paths processed together with operation result
   */
  def synchronize(sourceUri: String, targetUri: String, taskCount: Int = -1)(implicit spark: SparkSession): Array[FsOperationResult] = {
    val (missingInTarget, onlyInTarget) = getDelta(sourceUri,targetUri)

    LocalExecution.deletePaths(onlyInTarget.map(x => x.path))(spark.sparkContext.hadoopConfiguration)
    val paths = missingInTarget.map(x => Paths(x.path, x.path.replace(sourceUri, targetUri)))

    DistributedExecution.copyFiles(paths, taskCount)(spark)
  }

  /**
   * Returns tuple which contains: files missing in target, files only in target
   * @param sourceUri - source folder absolute path
   * @param targetUri - target folder absolute path
   * @return
   */
  def getDelta(sourceUri: String, targetUri: String): (Array[DeltaEntry],Array[DeltaEntry]) = {
    val srcFs = getFileSystem(config, sourceUri)
    val trgFs = getFileSystem(config, targetUri)
    val sourceFiles = listLevel(srcFs, new Path(sourceUri)).filter(!_.isDirectory)
    val targetFiles = listLevel(trgFs, new Path(targetUri)).filter(!_.isDirectory)
    val srcLocalPaths =  sourceFiles.map(x => FsElement(x.path.replaceAll(sourceUri,""),x.isDirectory,x.byteSize))
    val trgLocalPaths =  targetFiles.map(x => FsElement(x.path.replaceAll(targetUri,""),x.isDirectory,x.byteSize))
    val missingInTarget = srcLocalPaths.diff(trgLocalPaths).map(x => DeltaEntry(sourceUri+x.path,DeltaEntry.MISSING_IN_TARGET))
    val onlyInTarget = trgLocalPaths.diff(srcLocalPaths).map(x => DeltaEntry(targetUri+x.path,DeltaEntry.ONLY_IN_TARGET))
    (missingInTarget, onlyInTarget)
  }

}
