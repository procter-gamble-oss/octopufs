package com.pg.bigdata.octopufs

import com.pg.bigdata.octopufs.fs._
import com.pg.bigdata.octopufs.metastore._
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTable

package object metastore {

  def getTableLocation(tableName: String)(implicit spark: SparkSession): String = {
    getTableLocation(spark.catalog.currentDatabase,tableName)
  }
  def filterPartitions(db: String, tableName: String, partitionsToKeepLike: Seq[String])(implicit spark: SparkSession): Array[String] = {
    getTableL1PartitionsPaths(db,tableName).filter(x => partitionsToKeepLike.exists(y => x.contains(y)))
  }

  //returns absolute paths of partitions
  def getTableL1PartitionsPaths(db: String, tableName: String)(implicit spark: SparkSession): Array[String] = {
    val m = getTableMetadata(db,tableName).partitionColumnNames
    if(m.isEmpty) throw new Exception("Table " + db + "." + tableName + " is not partitioned")
    val absTblLoc = getTableLocation(db, tableName)
    val fs = getFileSystem(spark.sparkContext.hadoopConfiguration, absTblLoc)
    val partList = fs.listStatus(new Path(absTblLoc))
    partList.filter(_.isDirectory).map(absTblLoc + "/" + _.getPath.getName)
  }

  def getTableLocation(databaseName: String, tableName: String)(implicit spark: SparkSession): String = {
    val loc = getTableMetadata(databaseName, tableName).location.toString
    println(databaseName+"."+tableName+" location is "+loc)
    loc
  }

  private def getTableMetadata(databaseName: String, tableName: String)(implicit spark: SparkSession): CatalogTable = {
    spark.sessionState.catalog.getTableMetadata(new TableIdentifier(tableName, Option(databaseName)))
  }

  def refreshMetadata(db: String, table: String)(implicit spark: SparkSession): Unit = {
    val target = db + "." + table
    println("Refreshing metadata for " + target)
    spark.catalog.refreshTable(target)
    if (getTableMetadata(db, table).partitionColumnNames.nonEmpty) {
      println("Recovering partitions for " + target)
      spark.catalog.recoverPartitions(target)
    }
  }
   def getListOfTableFiles(sourceDbName: String, sourceTableName: String)(implicit spark: SparkSession): Array[String] = {
      spark.table(sourceDbName + "." + sourceTableName).inputFiles

    }

}
