Checkout examples in databricks_notebooks_examples branch
<BR>
Scaladoc link https://procter-gamble-tech.github.io/octopufs/#com.pg.bigdata.octopufs.package

## OctopuFS
OctopuFS is Scala/Spark toolkit to manage cloud storage, especially ADLSgen2 directly from databricks. It provides several capabilities, which internally have retry mechanism built in, which will repeat unsuccessful operations up to 5 times :
## File copy 
```com.pg.bigdata.octopufs.fs.DistributedExecution```
OctopuFS distributes copy operation to spark tasks and does data copy **3x faster** than spark read/write operation while utilizing less CPU
## Local multi-threaded operations
Many operations on ADLS are limited to HTTP requests only, thus they don't require significant fardware involvement and can be run on single machine. Operation on tens of thousands of files/folders take appox **1 minute**. There operations inclide:
# File move/rename/delete
```com.pg.bigdata.octopufs.fs.LocalExecution```
# Setting up ACLs on files and folders (recursively)
```com.pg.bigdata.octopufs.acl.AclManager```
# Getting size of files and folders
```com.pg.bigdata.octopufs.fs.getSize```
<br>
## Hive metadata operations 
```com.pg.bigdata.octopufs.Promotor```
OctopuFS uses above functions on Hive metadata layer (i.e. Tableas and partitions) to enable operations currently not accessible for tables, which are not using Databricks Delta format abstraction.
<BR><BR>
# Required setup of databricks cluster:
[RDD API security setup](https://docs.microsoft.com/en-us/azure/databricks/data/data-sources/azure/azure-datalake-gen2#rdd-api)
For copy operation only it is recommended to turn of or tune spark speculation ```spark.conf.set("spark.speculation","false")```
Most methods require implicit parameter:<BR>
  * SparkSession – for distributed copy ```implicit val s = spark```<BR>
  * Configuration – for local, multithreaded operation ```implicit val c = spark.sparkContext.hadoopConfiguration```<BR>

## How to get started
Clone and compile repository to get the latest version or download jar from artifact repositories. Once you have jar, upload it to spark cluster and run ot from scala notebook or from your own jar.<BR>
Please rememer to set up credentials like it was mentioned above.

## How to get help
In case you find anny issue with the package, do not hesitate to open issue on github. Please be as specific as possible regarding the error and context/environment you were using when issue occured.

## Maintainer
Jacek Tokar @ Procter&Gamble
