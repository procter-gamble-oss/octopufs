name := "Octopufs"

version := "0.1.{xxbuildversionxx}"

scalaVersion := "2.11.12"

val hadoopVersion = "2.7.3"
val sparkVersion = "2.4.6"

//MAKE SURE YOU DOWNLOAD SPARK LIBRARIES USING DATABRICKS CONNECT FOR YOUR RUNTIME VERSION AS SPARK VERSIONS OF
// DATABRICKS DO NOT MATCH OFFICIAL NUMBERING
unmanagedBase := file("/Users/jacektokar/miniconda3/lib/python3.7/site-packages/pyspark/jars")

libraryDependencies ++= Seq(
// "org.apache.spark" % "spark-sql_2.11" % sparkVersion % "provided",
// "org.apache.spark" % "spark-core_2.11" % sparkVersion % "provided",
 "org.pegdown" % "pegdown" % "1.6.0" % Test,
 "org.scalatest" %% "scalatest" % "3.0.5" % "provided",
  "org.apache.hadoop" % "hadoop-common" % hadoopVersion % "provided" ,
  "org.apache.hadoop" % "hadoop-hdfs" % hadoopVersion % "provided",
  "com.databricks" % "dbutils-api_2.11" % "0.0.4" % "provided"
)

parallelExecution in Test := false

/*
gitCommitString := git.gitHeadCommit.value.getOrElse("Not Set")
lazy val root = (project in file(".")).
  enablePlugins(BuildInfoPlugin).
  settings(
   buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion, buildInfoBuildNumber, gitCommitString),
   buildInfoOptions += BuildInfoOption.ToJson,
   buildInfoPackage := "buildInformation"
  )



buildInfoKeys ++= Seq[BuildInfoKey](
 resolvers,
 libraryDependencies in Test,
 //name_of_val -> task
 "hostname" -> java.net.InetAddress.getLocalHost().getHostName(),
 "whoami" -> System.getProperty("user.name"),
 BuildInfoKey.action("buildTimestamp") {
  java.text.DateFormat.getDateTimeInstance.format(new java.util.Date())
 }
)
//get git commit id
val gitCommitString = SettingKey[String]("gitCommit")
*/
credentials += Credentials(Path.userHome / "credentials.txt")

publishMavenStyle := true
publishArtifact in Test := false
publishTo := Some("app-nas-utils" at "https://pkgs.dev.azure.com/dh-platforms-devops/app-deng-nas_us/_packaging/anthill/maven/v1")

