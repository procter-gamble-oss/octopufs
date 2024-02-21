name := "Octopufs"

version := "0.2.{xxbuildversionxx}"

scalaVersion := "2.12.10"  //##change for runtime>6.4
//scalaVersion := "2.12.3"  //##change for runtime>7
//scalaVersion := "2.12.3"  //##change for runtime>6.4+

val hadoopVersion = "2.7.4"
val sparkVersion = "3.3.1" //##change for runtime>6.4

//MAKE SURE YOU DOWNLOAD SPARK LIBRARIES USING DATABRICKS CONNECT FOR YOUR RUNTIME VERSION AS SPARK VERSIONS OF
// DATABRICKS DO NOT MATCH OFFICIAL NUMBERING
//unmanagedBase := file("/Users/jacektokar/miniconda3/lib/python3.7/site-packages/pyspark/jars") //##change for runtime>6.4

libraryDependencies ++= Seq(
 "org.apache.spark" % "spark-sql_2.12" % sparkVersion % "provided",
 "org.apache.spark" % "spark-core_2.12" % sparkVersion % "provided",
 "org.pegdown" % "pegdown" % "1.6.0" % Test,
 "org.scalatest" %% "scalatest" % "3.0.5" % "provided",
  "org.apache.hadoop" % "hadoop-common" % hadoopVersion % "provided" ,
  "org.apache.hadoop" % "hadoop-hdfs" % hadoopVersion % "provided"//,
 //"com.google.code.findbugs" % "jsr305" % "1.3.+"
)

parallelExecution in Test := false

