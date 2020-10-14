name := "Octopufs"

version := "0.1.{xxbuildversionxx}"

scalaVersion := "2.11.12"  //##change for runtime>6.4
//scalaVersion := "2.12.3"  //##change for runtime>7
//scalaVersion := "2.12.3"  //##change for runtime>6.4+

val hadoopVersion = "2.7.3"
val sparkVersion = "2.4.6" //##change for runtime>6.4

//MAKE SURE YOU DOWNLOAD SPARK LIBRARIES USING DATABRICKS CONNECT FOR YOUR RUNTIME VERSION AS SPARK VERSIONS OF
// DATABRICKS DO NOT MATCH OFFICIAL NUMBERING
//unmanagedBase := file("/Users/jacektokar/miniconda3/lib/python3.7/site-packages/pyspark/jars") //##change for runtime>6.4

libraryDependencies ++= Seq(
 "org.apache.spark" % "spark-sql_2.11" % sparkVersion % "provided",
 "org.apache.spark" % "spark-core_2.11" % sparkVersion % "provided",
 "org.pegdown" % "pegdown" % "1.6.0" % Test,
 "org.scalatest" %% "scalatest" % "3.0.5" % "provided",
  "org.apache.hadoop" % "hadoop-common" % hadoopVersion % "provided" ,
  "org.apache.hadoop" % "hadoop-hdfs" % hadoopVersion % "provided",
  "com.databricks" % "dbutils-api_2.11" % "0.0.4" % "provided"
)

parallelExecution in Test := false

