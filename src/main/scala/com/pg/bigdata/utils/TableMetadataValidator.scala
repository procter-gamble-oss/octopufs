package com.pg.bigdata.utils

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.CatalogTable

object TableMetadataValidator {

  def getTableMetadata(databaseName: String, tableName: String)(implicit spark: SparkSession): CatalogTable = {
    import org.apache.spark.sql.catalyst.TableIdentifier
    spark.sessionState.catalog.getTableMetadata(new TableIdentifier(tableName, Option(databaseName)))
  }

  def validate(databaseName1: String, tableName1: String, databaseName2: String, tableName2: String)(implicit spark: SparkSession) = {
    val t1s = databaseName1+">"+tableName1
    val t2s = databaseName2+">"+tableName2
    val t1 = getTableMetadata(databaseName1, tableName1)
    val t2 = getTableMetadata(databaseName2, tableName2)
    if(!t1.partitionColumnNames.sameElements(t2.partitionColumnNames))
      throw new Exception("The two tables "+t1s+" and "+t2s+" have different partitioning columns")


  }
}
