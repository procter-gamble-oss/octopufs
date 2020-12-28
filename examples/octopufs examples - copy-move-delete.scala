// Databricks notebook source
// DBTITLE 1,Distributed copy of folder content
import com.pg.bigdata.octopufs.fs.DistributedExecution

spark.conf.set("spark.speculation","false")
implicit val s = spark
DistributedExecution.copyFolder("abfss://container@adlsname.dfs.core.windows.net/j/IRI","abfss://container@adlsname.dfs.core.windows.net/j/IRI2")

// COMMAND ----------

// DBTITLE 1,Distributed copy individual files
import com.pg.bigdata.octopufs.fs.Paths
import com.pg.bigdata.octopufs.fs.DistributedExecution

spark.conf.set("spark.speculation","false")
implicit val s = spark

val paths = Array(
  Paths("abfss://container@adlsname.dfs.core.windows.net/j/IRI/asd_20200111_20200516.zip","abfss://container@adlsname.dfs.core.windows.net/j/IRI/asd_20200111_20200516.zip2"),
  Paths("abfss://container@adlsname.dfs.core.windows.net/j/IRI/tes_20200111_20200516.zip","abfss://container@adlsname.dfs.core.windows.net/j/IRI/tes_20200111_20200516.zip2")
)
DistributedExecution.copyFiles(paths)

// COMMAND ----------

// DBTITLE 1,Move folder content 
import com.pg.bigdata.octopufs.fs.LocalExecution
implicit val conf = spark.sparkContext.hadoopConfiguration

LocalExecution.moveFolderContent("abfss://container@adlsname.dfs.core.windows.net/j/IRI2","abfss://container@adlsname.dfs.core.windows.net/j/IRI3")

// COMMAND ----------

// DBTITLE 1,Move individual files
import com.pg.bigdata.octopufs.fs.LocalExecution
implicit val conf = spark.sparkContext.hadoopConfiguration

val paths = Array(
  Paths("abfss://container@adlsname.dfs.core.windows.net/j/IRI/asd_20200111_20200516.zip2","abfss://container@adlsname.dfs.core.windows.net/j/IRI/asd_20200111_20200516.zip23"),
  Paths("abfss://container@adlsname.dfs.core.windows.net/j/IRI/tes_20200111_20200516.zip2","abfss://container@adlsname.dfs.core.windows.net/j/IRI/tes_20200111_20200516.zip23")
)

LocalExecution.movePaths(paths)

// COMMAND ----------

// DBTITLE 1,Delete files
import com.pg.bigdata.octopufs.fs.LocalExecution
implicit val conf = spark.sparkContext.hadoopConfiguration

val paths = Array(
  "abfss://container@adlsname.dfs.core.windows.net/j/IRI/asd_20200111_20200516.zip23",
  "abfss://container@adlsname.dfs.core.windows.net/j/IRI/tes_20200111_20200516.zip23"
)

LocalExecution.deletePaths(paths)

// COMMAND ----------

// DBTITLE 1,Delete folder or its content only
import com.pg.bigdata.octopufs.fs.LocalExecution
implicit val conf = spark.sparkContext.hadoopConfiguration

LocalExecution.deleteFolder("abfss://container@adlsname.dfs.core.windows.net/j/IRI3",deleteContentOnly=true) //deletes all files in the folder, but parent folder is not deleted
//LocalExecution.deleteFolder("abfss://container@adlsname.dfs.core.windows.net/j/IRI3") //delete folder and all it's files
