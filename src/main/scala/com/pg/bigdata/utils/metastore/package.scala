package com.pg.bigdata.utils

package object metastore {
  def getTableLocation(tableName: String)(implicit spark: SparkSession): String = {
    getTableLocation(spark.catalog.currentDatabase,tableName)
  }

}
