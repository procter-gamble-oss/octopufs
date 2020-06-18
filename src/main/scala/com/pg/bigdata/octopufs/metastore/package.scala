package com.pg.bigdata.octopufs

import com.pg.bigdata.octopufs.fs._
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTable

package object metastore {
  /**
   * Gets location of hive table in current database
   *
   * @param tableName
   * @param spark
   * @return String containing path to the table folder
   */
  def getTableLocation(tableName: String)(implicit spark: SparkSession): String = {
    getTableLocation(spark.catalog.currentDatabase, tableName)
  }

  /**
   * Returns a list of paths to partitions which name corresponds (contains) string(s) included in partitionsToKeepLike collection
   *
   * @param db                   - hive database name
   * @param tableName            - hive table name
   * @param partitionsToKeepLike - collection of partition names part for filtering
   * @param spark
   */
  def filterPartitions(db: String, tableName: String, partitionsToKeepLike: Seq[String])(implicit spark: SparkSession): Array[String] = {
    getTableL1PartitionsPaths(db, tableName).filter(x => partitionsToKeepLike.exists(y => x.contains(y)))
  }

  //returns absolute paths of partitions
  /**
   * Returns paths to first level partitions of a table
   *
   * @param db        - hive database name
   * @param tableName - hive table name
   * @param spark
   */
  def getTableL1PartitionsPaths(db: String, tableName: String)(implicit spark: SparkSession): Array[String] = {
    val m = getTableMetadata(db, tableName).partitionColumnNames
    if (m.isEmpty) throw new Exception("Table " + db + "." + tableName + " is not partitioned")
    val absTblLoc = getTableLocation(db, tableName)
    val fs = getFileSystem(spark.sparkContext.hadoopConfiguration, absTblLoc)
    val partList = fs.listStatus(new Path(absTblLoc))
    partList.filter(_.isDirectory).map(absTblLoc + "/" + _.getPath.getName)
  }

  /**
   * Returns location of a folder assigned to the hive table
   *
   * @param databaseName - hive database name
   * @param tableName    - hive table name
   * @param spark
   */
  def getTableLocation(databaseName: String, tableName: String)(implicit spark: SparkSession): String = {
    val loc = getTableMetadata(databaseName, tableName).location.toString
    println(databaseName + "." + tableName + " location is " + loc)
    loc
  }

  /**
   * Gets metadata of hive table
   *
   * @param databaseName
   * @param tableName
   * @param spark
   * @return
   */
  private def getTableMetadata(databaseName: String, tableName: String)(implicit spark: SparkSession): CatalogTable = {
    spark.sessionState.catalog.getTableMetadata(new TableIdentifier(tableName, Option(databaseName)))
  }

  /**
   * Refreshes hive metadata for hive table. It will also refresh partitions, if table is partitioned
   *
   * @param db    - hive database
   * @param table - hive table name
   * @param spark
   */
  def refreshMetadata(db: String, table: String)(implicit spark: SparkSession): Unit = {
    val target = db + "." + table
    println("Refreshing metadata for " + target)
    spark.catalog.refreshTable(target)
    if (getTableMetadata(db, table).partitionColumnNames.nonEmpty) {
      println("Recovering partitions for " + target)
      spark.catalog.recoverPartitions(target)
    }
  }

  /**
   * Gets array of paths to files of a table. The list of files comes from hive metastore cache.
   * @param sourceDbName
   * @param sourceTableName
   * @param spark
   */
  def getListOfTableFiles(sourceDbName: String, sourceTableName: String)(implicit spark: SparkSession): Array[String] = {
    spark.table(sourceDbName + "." + sourceTableName).inputFiles

  }

}
