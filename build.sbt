name := "Promoter"

version := "0.1"

scalaVersion := "2.11.12"

val sparkVersion = "2.4.4"

unmanagedBase := file("/Users/jacektokar/miniconda3/lib/python3.7/site-packages/pyspark/jars")

libraryDependencies ++= Seq(

 "org.apache.spark" % "spark-sql_2.11" % sparkVersion % "provided",
 "org.apache.spark" % "spark-core_2.11" % sparkVersion % "provided",
 "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",


  //  "org.slf4j" % "slf4j-api" % "1.7.5",
  //  "org.slf4j" % "slf4j-simple" % "1.7.5",

  // hadoop
  "org.apache.hadoop" % "hadoop-common" % "2.7.3" % "provided" ,
 // "org.apache.commons" % "commons-io" % "1.3.2" % "provided" ,
  "org.apache.hadoop" % "hadoop-hdfs" % "2.7.3" % "provided",
  "com.databricks" % "dbutils-api_2.11" % "0.0.4" % "provided"

)

