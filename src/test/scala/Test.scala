import com.pg.bigdata.octopufs.acl.AclManager
import com.pg.bigdata.octopufs.acl.AclManager.FsPermission
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.pg.bigdata.octopufs.fs._
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.permission.{AclEntry, AclEntryScope}
import scala.collection.JavaConverters._
import scala.util.Random

object Test extends App {
 implicit val spark: SparkSession = SparkSession.builder().
    appName("NAS_").
    master("local").
    getOrCreate()
  DataMockup.prepareData()
  //Promotor.copyFilesBetweenTables("STORE_SALES_PXM_FCT", "STORE_SALES_TARGET",200)(spark)

}


object DataMockup {

  def prepareFileAndFolderStructure(spark: SparkSession) = {
    val basePath = "hdfs://localhost:8020/"
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = getFileSystem(conf,basePath)
    val root = basePath+"pattern"
    val depth = 3

    def generatePathTree(level: Int, parents: Array[String]): Array[String] = {
      //val randomArray =
      val children = parents.flatMap(parent => new Random().alphanumeric.take(new Random().nextInt(5)).map(x => parent + "/"+ Random.alphanumeric.take(6).mkString))
      if(children.isEmpty) throw new Exception("puste dzieci")
       if(level>depth) return children
       else children ++ generatePathTree(level+1, children)
    }

    val folders = generatePathTree(0, Array(root))

    val files = folders.map(x => x+"/"+x.split("/").last+".txt")
    folders.foreach(println)
    files.foreach(println)
    println("creating folders")
    val mkdir = folders.map(x => fs.mkdirs(new Path(x)))
    if(mkdir.count(!_)>0) throw new Exception("could not create some folders")
    println("creating files")
    val newFiles = files.map(x => fs.createNewFile(new Path(x)))
    if(newFiles.count(!_)>0) throw new Exception("could not create some files")

    def buildRandomAclForPath(path: String, scopeType: String = "ACCESS") = {
      val scope = if(scopeType == "ACCESS") "ACCESS" else "DEFAULT"
      val typeOfAccess = (List("r","-"))(new Random().nextInt(2))+
        (List("w","-"))(new Random().nextInt(2)) +
        (List("x","-"))(new Random().nextInt(2))
      val p = AclManager.FsPermission("user", typeOfAccess,"ACCESS",path.split("/").last)
      AclManager.getAclEntry(p)

    }
    println("copy 80% of blank files structure")

    val sourceFiles = new Random().shuffle(files.toList).take((files.length*0.8).toInt)
    val targetFiles = sourceFiles.map(_.replace("pattern","hereIsTheNewOne"))

    sourceFiles.map(x => org.apache.hadoop.fs.FileUtil.copy(fs, new Path(x), fs, new Path(x.replace("pattern","hereIsTheNewOne")), false, conf))

    println("Setting folders ACLs")
    import scala.util.Random
    new Random().shuffle(folders.toList).take((folders.length*0.8).toInt). map(x => fs.modifyAclEntries(new Path(x),List(buildRandomAclForPath(x, "DEFAULT")).asJava))
    new Random().shuffle(folders.toList).take((folders.length*0.8).toInt). map(x => fs.modifyAclEntries(new Path(x),List(buildRandomAclForPath(x)).asJava))
    new Random().shuffle(files.toList).take((files.length*0.8).toInt). map(x => fs.modifyAclEntries(new Path(x),List(buildRandomAclForPath(x)).asJava))
    println(fs.getAclStatus(new Path(folders.head)))
    println("cleanup")
    folders.map(x => fs.delete(new Path(x), true))
    folders.map(x => fs.delete(new Path(basePath+"hereIsTheNewOne"), true))
  }

  def prepareData()(implicit spark: SparkSession): Unit = {
      readCSV("data/store_sales_pxm_fct.csv").coalesce(10).write.option("path","data/STORE_SALES_PXM_FCT").saveAsTable("STORE_SALES_PXM_FCT")
      readCSV("data/store_sales_pxm_fct.csv").coalesce(10).where("1=2").write.option("path","Test.scaladata/STORE_SALES_TARGET").saveAsTable("STORE_SALES_TARGET")
  }
  def readCSV(path: String)(implicit spark: SparkSession): DataFrame = {
    spark.read.format("csv").
      option("header", "true").
      option("inferSchema", "true").
      load(path)
  }

}

