package com.pg.bigdata.octopufs.metastore

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.CatalogTable

object TableMetadataValidator {

  def validate(tableName1: String, tableName2: String)(implicit spark: SparkSession): Unit =
    validate(spark.catalog.currentDatabase, tableName1, spark.catalog.currentDatabase, tableName2)

  def validate(databaseName1: String, tableName1: String, databaseName2: String, tableName2: String)(implicit spark: SparkSession): Unit = {
    val t1s = databaseName1+"."+tableName1
    val t2s = databaseName2+"."+tableName2
    val t1 = getTableMetadata(databaseName1, tableName1)
    val t2 = getTableMetadata(databaseName2, tableName2)
    if(!t1.partitionColumnNames.equals(t2.partitionColumnNames))
      throw new Exception("The two tables "+t1s+" and "+t2s+" have different partitioning columns")

    if(!t1.schema.equals(t2.schema))
      throw new Exception("Schemas in tables "+t1s+" and "+t2s+" are different")

    if(t1.storage.compressed!=t2.storage.compressed)
      throw new Exception("Compression in tables "+t1s+" and "+t2s+" are different")

    if(t1.storage.inputFormat.getOrElse("") != t2.storage.inputFormat.getOrElse(""))
      throw new Exception("InputFormats in tables "+t1s+" and "+t2s+" are different")

    if(t1.storage.outputFormat.getOrElse("") != t2.storage.outputFormat.getOrElse(""))
      throw new Exception("OutputFormats in tables "+t1s+" and "+t2s+" are different")
  }

  def getTableMetadata(databaseName: String, tableName: String)(implicit spark: SparkSession): CatalogTable = {
    import org.apache.spark.sql.catalyst.TableIdentifier
    spark.sessionState.catalog.getTableMetadata(new TableIdentifier(tableName, Option(databaseName)))
  }
}
