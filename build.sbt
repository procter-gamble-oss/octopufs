name := "Octopufs"

version := "0.3.{xxbuildversionxx}"

scalaVersion := "2.13.16"

val sparkVersion = "4.0.0"
val hadoopVersion = "3.3.6"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
  "org.apache.hadoop" % "hadoop-common" % hadoopVersion % Provided,
  "org.apache.hadoop" % "hadoop-hdfs" % hadoopVersion % Provided,

  "org.scalatest" %% "scalatest" % "3.2.18" % Test,
  "org.apache.hadoop" % "hadoop-common" % hadoopVersion % Test,
  "org.apache.hadoop" % "hadoop-hdfs" % hadoopVersion % Test,
  "org.apache.hadoop" % "hadoop-minicluster" % hadoopVersion % Test,
  "org.apache.hadoop" % "hadoop-auth" % hadoopVersion % Test,
  "javax.servlet" % "javax.servlet-api" % "3.1.0" % Test,
  "org.mockito" % "mockito-core" % "4.11.0" % Test,
  "junit" % "junit" % "4.13.2" % Test
)

javacOptions ++= Seq("-release", "17")

Test / parallelExecution := false
Test / classLoaderLayeringStrategy := ClassLoaderLayeringStrategy.ScalaLibrary
Test / classLoaderLayeringStrategy := ClassLoaderLayeringStrategy.Flat

