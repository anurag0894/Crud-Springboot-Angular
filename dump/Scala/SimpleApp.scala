import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.DataFrame
import java.io.File
import java.text.SimpleDateFormat
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import scala.collection.{ Iterator, mutable }
import java.util.{Hashtable, Properties}
 import java.text.SimpleDateFormat
import org.apache.log4j.{Level,LogManager}




object SimpleApp{
def tblInsert(spark:SparkSession,tblName: String  )  = {
  // Function: Inserting into Internal Hive table + Insert into Audit table

  try {
    println(tblName + "  partition:    " )
    val befTime = System.currentTimeMillis()	
    val df = spark.sql("select * from " + tblName + "__x").coalesce(5)
    df.write.format("orc").mode("overwrite").saveAsTable(tblName)
    val resultCount =  spark.read.format("orc").table(tblName).count
    val f = new SimpleDateFormat("yyyy-M-dd hh:mm:ss")
    var afTime=System.currentTimeMillis()
    var timeTaken = afTime - befTime
    val timeelapsed = timeTaken / (1000)
    //spark.sql("insert into table g00103.audit_table values('"+ tblName +"',"+ resultCount(0) + ",'" + f.format(befTime) + "','" + f.format(afTime) + "','" + timeelapsed + "','"+ "completed" + "')")
  }
  catch {
    case e: Exception => { println("Error-----" + tblName) }
  }
}
def main(args:Array[String]){

val log = LogManager.getRootLogger
                log.setLevel(Level.WARN)

val warehouseLocation = new File("spark-warehouse").getAbsolutePath

 val spark = SparkSession.builder
      .appName("my-spark-app")
      .config("spark.some.config.option", "config-value")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()

	import spark.implicits._
val sourceSchema = "g00103"
val sourceMasterTableName = "gs_iqp_master_table"
val hdfs: org.apache.hadoop.fs.FileSystem =
  org.apache.hadoop.fs.FileSystem.get(
    new org.apache.hadoop.conf.Configuration())
val  qualified_tbName = sourceSchema + "." + sourceMasterTableName
val dsTables = spark.sql("select table_name from " + qualified_tbName ).as[String]
dsTables.collect().foreach{tblName:String =>  tblInsert(spark,sourceSchema+"."+tblName) 
}

}}

  
