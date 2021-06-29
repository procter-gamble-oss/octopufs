package com.pg.bigdata.octopufs

import com.pg.bigdata.octopufs.Assistant._
import com.pg.bigdata.octopufs.fs._
import com.pg.bigdata.octopufs.metastore._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession

//val magicPrefix = ".dfs.core.windows.net"

object Promotor extends Serializable {

  /**
   * This function copies files between tables without overwriting target folder itself. It allows to keep proper default ACLs on files in the target.
   * Execution is distributed to entire cluster so performance will depend on number of cores and VMs network throughput. This function assumes that both tables are in the same hive CURRENT database spark.catalog.currentDatabase
   *
   * @param sourceTableName - source table name
   * @param targetTableName - target table name
   * @param taskCount       - number of threads to distribute the load to. It equals number of files by default which ensures best distribution, however it may trigger a lot of small tasks if your files are small.
   * @param spark           - spark session required to access hive metastore and to run distributed jobs.
   * @return FSOperationResult which contains all copied paths together with copy result (isSuccess)
   */
  def copyFilesBetweenTables(sourceTableName: String, targetTableName: String, taskCount: Int = -1)
                            (implicit spark: SparkSession): Array[FsOperationResult] = {
    val db = spark.catalog.currentDatabase
    copyFilesBetweenTables(db, sourceTableName, db, targetTableName, taskCount)
  }

  /**
   * This function copies partitions of a table to different table. Only first level partitioning is taken into account. Both tables must be in the same,
   * current database spark.catalog.currentDatabase. Function does NOT delete any files or folders - it appends all the files and folders are created automatically, if needed.
   * It will run as many tasks as there are files.
   *
   * @param sourceTableName       - source table name
   * @param targetTableName       - target table name
   * @param matchStringPartitions - sequence of string which is used to match partitions folder names. The match is done using String "contains" function.
   * @param spark                 - spark session required to access hive metastore and to run distributed jobs.
   * @return Array of FSOperationResult which contains all copied paths together with copy result (isSuccess)
   */
  def copyTablePartitions(sourceTableName: String, targetTableName: String, matchStringPartitions: Seq[String])
                         (implicit spark: SparkSession): Array[FsOperationResult] = {
    copyTablePartitions(spark.catalog.currentDatabase, sourceTableName, spark.catalog.currentDatabase,
      targetTableName, matchStringPartitions)
  }

  /**
   * This function deletes and copies partitions from source table to target. Only first level partitioning is taken into account. Both tables must be in the same,
   * current database spark.catalog.currentDatabase. Delete operation is executed on the target partition folders, not individual files, so you may want to restore
   * ACLs if different partitions require different security settings. It will run as many tasks as there are files.
   *
   * @param sourceTableName       - source table name
   * @param targetTableName       - target table name
   * @param matchStringPartitions - sequence of string which is used to match partitions folder names. The match is done using String "contains" function. Works only for 1st level partitioning.
   * @param spark                 - spark session required to access hive metastore and to run distributed jobs.
   * @return Array of FSOperationResult which contains all copied paths together with copy result (isSuccess)
   */
  def copyOverwritePartitions(sourceTableName: String, targetTableName: String, matchStringPartitions: Seq[String])
                             (implicit spark: SparkSession): Array[FsOperationResult] = {
    val db = spark.catalog.currentDatabase
    deleteTablePartitions(db, targetTableName, matchStringPartitions)(spark)
    copyTablePartitions(db, sourceTableName, db, targetTableName, matchStringPartitions)
  }

  //if target folder exists, it will be deleted first

  /**
   * Function copies all files from source table to target table. All files from target table are deleted first (target folder remains intact). Both
   * tables must be in the same, current database spark.catalog.currentDatabase.
   *
   * @param sourceTableName - source table name
   * @param targetTableName - target table name
   * @param spark           - spark session required to access hive metastore and to run distributed jobs.
   * @return Array of FSOperationResult which contains all copied paths together with copy result (isSuccess)
   */
  def copyOverwriteTable(sourceTableName: String, targetTableName: String)
                        (implicit spark: SparkSession, confEx: Configuration): Array[FsOperationResult] = {
    val db = spark.catalog.currentDatabase
    copyOverwriteTable(db, sourceTableName, db, targetTableName)
  }

  /**
   * Function copies all files from source table to target table. All files from target table are deleted first (target folder remains intact).
   *
   * @param sourceDbName    - hive metastore database where source table is defined
   * @param sourceTableName - source table name
   * @param targetDbName    - hive metastore database where source table is defined
   * @param targetTableName - target table name
   * @param taskCount       - number of threads to distribute the load to. It equals number of files by default, which ensures best distribution, however it may trigger a lot of small tasks if your files are small.
   * @param spark           - spark session required to access hive metastore and to run distributed jobs.
   * @return
   */
  def copyOverwriteTable(sourceDbName: String, sourceTableName: String, targetDbName: String, targetTableName: String, taskCount: Int = -1)
                        (implicit spark: SparkSession): Array[FsOperationResult] = {
    val trgLoc = getTableLocation(targetDbName, targetTableName)
    LocalExecution.deleteFolder(trgLoc, true)(spark.sparkContext.hadoopConfiguration)
    val res = copyFilesBetweenTables(sourceDbName, sourceTableName, targetDbName, targetTableName, taskCount) //todo rethink approach to partition count
    refreshMetadata(targetDbName, targetTableName)
    res
  }

  /**
   * This function copies files between tables without overwriting target folder itself. It allows to keep proper default ACLs on files in the target.
   * Execution is distributed to entire cluster so performance will depend on number of cores and VMs network throughput.
   *
   * @param sourceDbName    - hive metastore database where source table is defined
   * @param sourceTableName - source table name
   * @param targetDbName    - hive metastore database where source table is defined
   * @param targetTableName - target table name
   * @param taskCount       - number of threads to distribute the load to. Value -1 ensures best distribution (one file per task), however it may trigger a lot of small tasks if your files are small.
   * @param spark           - spark session required to access hive metastore and to run distributed jobs.
   * @return Array of FSOperationResult which contains all copied paths together with copy result (isSuccess)
   */
  def copyFilesBetweenTables(sourceDbName: String, sourceTableName: String, targetDbName: String, targetTableName: String, taskCount: Int)
                            (implicit spark: SparkSession): Array[FsOperationResult] = {
    val paths = getTablesPathsList(sourceDbName, sourceTableName, targetDbName, targetTableName) //.take(5)
    if (paths.isEmpty) {
      //throw new Exception("No files to be copied")
      println("No files to be copied for table  " + sourceDbName + "." + sourceTableName)
      Array[FsOperationResult]()
    }

    val srcLoc = getTableLocation(sourceDbName, sourceTableName)
    val trgLoc = getTableLocation(targetDbName, targetTableName)
    DistributedExecution.copyFolder(srcLoc, trgLoc, taskCount)
  }

  //todo: scaladoc
  def copyOverwriteSelectedSubfoldersContent(sourcePathUri: String, targetPathUri: String, matchStringPaths: Seq[String],
                                             taskCount: Int = -1)(implicit spark: SparkSession) = {
    val subFolders = getSubfolderPaths(sourcePathUri)
    val filteredPaths = filterPaths(subFolders, matchStringPaths)

    val fs = getFileSystem(spark.sparkContext.hadoopConfiguration, targetPathUri)
    val pathsToBeDeleted = filteredPaths.map(x => x.replace(sourcePathUri, targetPathUri)).filter(x => fs.exists(new Path(x)))
    if (pathsToBeDeleted.nonEmpty) {
      println("Subfolders of " + targetPathUri + " which are going to be deleted (if exist):")
      pathsToBeDeleted.foreach(println)
      LocalExecution.deletePaths(pathsToBeDeleted)(spark.sparkContext.hadoopConfiguration)
    }
    copySelectedSubFoldersContent(sourcePathUri, targetPathUri, matchStringPaths, taskCount)
  }

  //todo: scaladoc
  def copySelectedSubFoldersContent(sourcePathUri: String, targetPathUri: String, matchStringPaths: Seq[String],
                                    taskCount: Int = -1)
                                   (implicit spark: SparkSession): Array[FsOperationResult] = {

    val subFolders = getSubfolderPaths(sourcePathUri)
    val filteredPaths = filterPaths(subFolders, matchStringPaths)
    if (filteredPaths.isEmpty)
      throw new Exception("There are no partitions to be copied. Please check your input parameters or table content")

    println("Sub-folders to be copied: " + filteredPaths.mkString(", "))

    implicit val fs = getFileSystem(spark.sparkContext.hadoopConfiguration, sourcePathUri)
    val allSourceFiles = getFilesOnlyOfFolders(filteredPaths)
    val sourceTargetPaths = allSourceFiles.map(x => Paths(x.path, x.path.replace(sourcePathUri, targetPathUri)))
    if (sourceTargetPaths.isEmpty)
      throw new Exception("There are no files to be copied. Please check your input parameters or table content")
    println("Files of " + sourcePathUri + " which are going to be copied to " + targetPathUri + ":")
    sourceTargetPaths.slice(0, 5).foreach(x => println(x))
    DistributedExecution.copyFiles(sourceTargetPaths, taskCount)
  }

  //todo: scaladoc
  def moveSelectedSubFolders(sourcePathUri: String, targetPathUri: String, matchStringPaths: Seq[String])
                            (implicit spark: SparkSession) = {
    val subFolders = getSubfolderPaths(sourcePathUri)
    val filteredPaths = filterPaths(subFolders, matchStringPaths)
    println("Subfolders of " + sourcePathUri + " which are going to be moved:")
    filteredPaths.foreach(println)
    moveFolders(filteredPaths, targetPathUri)
  }

  private def moveFolders(foldersToBeMovedUri: Array[String], destinationUri: String)(implicit spark: SparkSession): Array[FsOperationResult] = {
    if (foldersToBeMovedUri.isEmpty) {
      throw new Exception("There is nothing to be moved (array provided is empty)")
    }
    val sourceBaseUri = new Path(foldersToBeMovedUri.head).getParent.toString
    implicit val conf: Configuration = spark.sparkContext.hadoopConfiguration
    implicit val fs = getFileSystem(conf, foldersToBeMovedUri.head)
    val trgFs = getFileSystem(conf, destinationUri)
    val sourceTargetUriPaths = foldersToBeMovedUri.map(x => Paths(x, x.replace(sourceBaseUri, destinationUri)))
    sourceTargetUriPaths.foreach(x =>
      if (!doesMoveLookSafe(fs, x.sourcePath, x.targetPath)) {
        throw new Exception("It looks like there is a path in source " + x.sourcePath + " which is empty, but target folder " + x.targetPath + " is not. " +
          "Promotor tries to protect you from unintentional deletion of your data in the target. If you want to avoid this exception, " +
          "place at least one empty file in the source folder to avoid run interruption")
      }
    )

    checkIfFsIsTheSame(fs, trgFs) //throws exception in case of discrepancy

    val existingTargetFolders = sourceTargetUriPaths.map(_.targetPath).
      filter(folder => fs.exists(new Path(folder)))

    val transaction = new SafetyFuse(sourceBaseUri)
    if (!transaction.isInProgress()) {
      transaction.startTransaction()
      if(existingTargetFolders.nonEmpty) {
        println("Deleting targets... (showing 10 sample paths")
        existingTargetFolders.slice(0, 10).foreach(println)
        LocalExecution.deletePaths(existingTargetFolders)
      }
    }

    println("Now moving source... Showing first 10 paths")
    sourceTargetUriPaths.slice(0, 10).foreach(println)
    val res = LocalExecution.movePaths(sourceTargetUriPaths)
    transaction.endTransaction()

    res
  }

  /**
   * This function deletes partitions from the target table (if exist and match matchStringPartitions) and copies partitions from source table to target. Only first level partitioning is taken into account.
   *
   * @param sourceDbName          - source database name
   * @param sourceTableName       - source table name
   * @param targetDbName          - target database name
   * @param targetTableName       - target table name
   * @param matchStringPartitions - sequence of string which is used to match partitions folder names. The match is done using String "contains" function. Works only for 1st level partitioning.
   * @param taskCount             - number of threads to distribute the load to. Value -1 ensures best distribution (one file per task), however it may trigger a lot of small tasks if your files are small.
   * @param spark                 - spark session required to access hive metastore and to run distributed jobs.
   * @return Array of FSOperationResult which contains all copied paths together with copy result (isSuccess)
   */
  def copyOverwritePartitions(sourceDbName: String, sourceTableName: String, targetDbName: String, targetTableName: String, matchStringPartitions: Seq[String],
                              taskCount: Int = -1)
                             (implicit spark: SparkSession): Array[FsOperationResult] = {
    deleteTablePartitions(targetDbName, targetTableName, matchStringPartitions)(spark)
    copyTablePartitions(sourceDbName, sourceTableName, targetDbName, targetTableName, matchStringPartitions, taskCount)
  }

  /**
   * This function copies partitions from source table to target. Only first level partitioning is taken into account.
   *
   * @param sourceDbName          - source database name
   * @param sourceTableName       - source table name
   * @param targetDbName          - target database name
   * @param targetTableName       - target table name
   * @param matchStringPartitions - sequence of string which is used to match partitions folder names. The match is done using String "contains" function. Works only for 1st level partitioning.
   * @param taskCount             - number of threads to distribute the load to. Value -1 ensures best distribution (one file per task), however it may trigger a lot of small tasks if your files are small.
   * @param spark                 - spark session required to access hive metastore and to run distributed jobs.
   * @return Array of FSOperationResult which contains all copied paths together with copy result (isSuccess)
   */
  def copyTablePartitions(sourceDbName: String, sourceTableName: String, targetDbName: String, targetTableName: String, matchStringPartitions: Seq[String],
                          taskCount: Int = -1)
                         (implicit spark: SparkSession): Array[FsOperationResult] = {
    val paths = filterPartitions(sourceDbName, sourceTableName, matchStringPartitions)
    if (paths.isEmpty)
      throw new Exception("There are no files to be copied. Please check your input parameters or table content")

    println("Partitions to be copied: " + paths.mkString("\n"))
    val sourceAbsTblLoc = getTableLocation(sourceDbName, sourceTableName)
    val targetAbsTblLoc = getTableLocation(targetDbName, targetTableName)

    val fs = getFileSystem(spark.sparkContext.hadoopConfiguration, sourceAbsTblLoc)
    val allSourceFiles = getFilesOnlyOfFolders(paths)(fs)

    val sourceTargetPaths = allSourceFiles.map(x => Paths(x.path, x.path.replace(sourceAbsTblLoc, targetAbsTblLoc)))
    println("Files of partitions of table " + sourceDbName + "." + sourceTableName + " which are going to be copied to " + targetDbName + "." + targetTableName + ":")
    sourceTargetPaths.slice(0, 5).foreach(x => println(x)) //debug
    val res = DistributedExecution.copyFiles(sourceTargetPaths, taskCount)
    refreshMetadata(targetDbName, targetTableName)
    res
  }

  /**
   * Function deletes partitions of a table
   *
   * @param db                    - Hive database, where the table is defined
   * @param tableName             - Hive table name
   * @param matchStringPartitions - sequence of string which is used to match partitions folder names. The match is done using String "contains" function. Works only for 1st level partitioning.
   * @param spark                 - spark session to get access to Hive metastore
   * @return Array of FSOperationResult which contains all copied paths together with copy result (isSuccess)
   */
  def deleteTablePartitions(db: String, tableName: String, matchStringPartitions: Seq[String])(implicit spark: SparkSession): Array[FsOperationResult] = {
    val paths = filterPartitions(db, tableName, matchStringPartitions)
    println("Partitions of table " + db + "." + tableName + " which are going to be deleted:")
    paths.foreach(println)
    val res = LocalExecution.deletePaths(paths)(spark.sparkContext.hadoopConfiguration)
    refreshMetadata(db, tableName)
    res
  }

  /**
   * Moves partition from one table to another. This is purely metadata operation on ADLSgen2 thus it runs on a driver and does not use workers.
   *
   * @param sourceTableName       - source table name
   * @param targetTableName       - target table name
   * @param matchStringPartitions - sequence of string which is used to match partitions folder names. The match is done using String "contains" function. Works only for 1st level partitioning.
   * @param spark                 - spark session required to access hive metastore to get list of files
   * @return Array of FSOperationResult which contains all copied paths together with copy result (isSuccess)
   */
  def moveTablePartitions(sourceTableName: String, targetTableName: String, matchStringPartitions: Seq[String])
                         (implicit spark: SparkSession): Array[FsOperationResult] = {
    val db = spark.catalog.currentDatabase
    moveTablePartitions(db, sourceTableName, db, targetTableName, matchStringPartitions)(spark)
  }

  /**
   * Function will delete existing (and overlapping) partitions from the target table and move partitions from source to the target. Please note that
   * move operation does NOT modify ACLs, so you may want to assign ACLs after the move. Function runs on driver node, because move and delete operations
   * are metadata modifications on ADLSg2
   *
   * @param sourceDbName          - source database name
   * @param sourceTableName       - source table name
   * @param targetDbName          - target database name
   * @param targetTableName       - target table name
   * @param matchStringPartitions - sequence of string which is used to match partitions folder names. The match is done using String "contains" function. Works only for 1st level partitioning.
   * @param spark                 - spark session required to access hive metastore.
   * @return Array of FSOperationResult which contains all copied paths together with copy result (isSuccess)
   */
  def moveTablePartitions(sourceDbName: String, sourceTableName: String, targetDbName: String, targetTableName: String,
                          matchStringPartitions: Seq[String])
                         (implicit spark: SparkSession): Array[FsOperationResult] = {
    implicit val conf: Configuration = spark.sparkContext.hadoopConfiguration
    val partitionFoldersUriPaths = filterPartitions(sourceDbName, sourceTableName, matchStringPartitions)
    val sourceAbsTblLoc = getTableLocation(sourceDbName, sourceTableName)
    val targetAbsTblLoc = getTableLocation(targetDbName, targetTableName)

    if (partitionFoldersUriPaths.isEmpty) {
      println("There is nothing to be moved (source " + sourceAbsTblLoc + "is empty)")
      return Array[FsOperationResult]()
    }

    println("Partitions of table " + sourceDbName + "." + sourceTableName + " which are going to be moved to " + targetDbName + "." + targetTableName + ":")

    val res = moveFolders(partitionFoldersUriPaths, targetAbsTblLoc)

    refreshMetadata(sourceDbName, sourceTableName)
    refreshMetadata(targetDbName, targetTableName)

    res
  }

  /**
   * Function moves files of one table to another one. It will delete all files from target table. Function relies on hive metadata (it will not scan the storage)
   *
   * @param sourceTableName - source table name
   * @param targetTableName - target table name
   * @param spark           - spark session required to access hive metastore to get list of files
   * @return Array of FSOperationResult which contains all copied paths together with copy result (isSuccess)
   */
  def moveFilesBetweenTables(sourceTableName: String, targetTableName: String)
                            (implicit spark: SparkSession): Array[FsOperationResult] = {
    val db = spark.catalog.currentDatabase
    moveFilesBetweenTables(db, sourceTableName, db, targetTableName)(spark)
  }

  /**
   * Function moves files of one table to another one. It will delete all files from target table. Function relies on hive metadata (it will not scan the storage)
   *
   * @param sourceDbName    - source database name
   * @param sourceTableName - source table name
   * @param targetDbName    - target database name
   * @param targetTableName - target table name
   * @param spark           - spark session required to access hive metastore to get list of files
   * @return Array of FSOperationResult which contains all copied paths together with copy result (isSuccess)
   */
  def moveFilesBetweenTables(sourceDbName: String, sourceTableName: String, targetDbName: String, targetTableName: String)
                            (implicit spark: SparkSession): Array[FsOperationResult] = {
    val srcLoc = getTableLocation(sourceDbName, sourceTableName)
    val trgLoc = getTableLocation(targetDbName, targetTableName)
    val res = LocalExecution.moveFolderContent(srcLoc, trgLoc, keepSourceFolder = true)(spark.sparkContext.hadoopConfiguration)
    refreshMetadata(sourceDbName, sourceTableName)
    refreshMetadata(targetDbName, targetTableName)
    res
  }

}
