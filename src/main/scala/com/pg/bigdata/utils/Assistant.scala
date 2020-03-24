package com.pg.bigdata.utils


import java.util.concurrent.Executors

import com.pg.bigdata.utils.fs.{FSElement, Paths}
import com.pg.bigdata.utils.fs._
import com.pg.bigdata.utils.metastore._

import org.apache.spark.sql.SparkSession


object Assistant {

  def getTablesPathsList(sourceDbName: String, sourceTableName: String,
                         targetDbName: String, targetTableName: String)
                        (implicit spark: SparkSession): Array[Paths] = {
    val sourceLocation = getTableLocation(sourceDbName,sourceTableName)
    val targetLocation = getTableLocation(targetDbName,targetTableName)
    println("target location " +targetLocation)
    val sourceFileList = getListOfTableFiles(sourceDbName, sourceTableName)
    if(sourceFileList.isEmpty)
      throw new Exception("There is nothing to be copied")
    val targetFileList = sourceFileList.map(_.replaceAll(sourceLocation, targetLocation))
    println(targetFileList(0))
    sourceFileList.zip(targetFileList).map(x => Paths(x._1,x._2))
  }






}
