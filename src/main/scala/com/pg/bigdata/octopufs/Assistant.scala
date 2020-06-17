package com.pg.bigdata.octopufs


//import buildInformation.BuildInfo
import com.pg.bigdata.octopufs.fs.{FsElement, Paths}
import com.pg.bigdata.octopufs.metastore._
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
/*

  def buildInfo(): Unit = {
    //logger definition
    println("-----------------info about last build:-----------------")
    println("name: " + BuildInfo.name)
    println("buildTimestamp: " + BuildInfo.buildTimestamp)
    println("hostname: " + BuildInfo.hostname)
    println("whoami: " + BuildInfo.whoami)
    println("version: " + BuildInfo.version)
    println("scalaVersion: " + BuildInfo.scalaVersion)
    println("sbtVersion: " + BuildInfo.sbtVersion)
    println("buildInfoNumber: " + BuildInfo.buildInfoBuildNumber.toString)
    println("gitCommit: " + BuildInfo.gitCommit)
    println("resolvers: " + BuildInfo.resolvers)
    println("libraryDependencies: " + BuildInfo.test_libraryDependencies)
    println("---------------------------end---------------------------")
  }
*/


}
