import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import scala.collection.mutable.ArrayBuffer
import java.text.SimpleDateFormat
import org.apache.log4j.{Level,LogManager}
import java.io.File

case class Audit(batch_number: String, source_tbl_name: String, des_tbl_name: String, refresh_mode: String, refresh_rec_count: Long,
                              refresh_start_time: String, refresh_end_time: String, refresh_status: String, refresh_runtime: Long, message: String)


object Master_quality {

  // Function to create an Intance of Audit record
  def auditRecord(batch_number: String, source_tbl_name: String, des_tbl_name: String,
                  refresh_mode: String, refresh_rec_count: Long, refresh_start_time: Long,
                  refresh_end_time: Long, refresh_status: String, refresh_runtime: Long, message: String):Audit = {
            val dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
            new Audit(
            batch_number,
            source_tbl_name,
            des_tbl_name,
            refresh_mode,
            refresh_rec_count,
            if (refresh_start_time != 0) dateFormat.format(refresh_start_time) else "",
            if (refresh_end_time != 0) dateFormat.format(refresh_end_time) else "",
            refresh_status,
            refresh_runtime,
            message
        )
}

  
def tblInsert(spark:SparkSession,batch_Num:String,schemaName: String, des_tblName: String,ingestion_query: String, numPartition:Int,refreshMode: String,fileFormat:String) = {

val src_tblName = "NA"
try{
	val before_timestamp: Long = System.currentTimeMillis()

	val result = spark.sql(ingestion_query)

	val df_partition_size = result.rdd.partitions.size
	val final_partition_num = if (numPartition <= 0) df_partition_size else numPartition

	val resultWithPartit = result.coalesce(final_partition_num)
	resultWithPartit.createOrReplaceTempView("qualityTable") 



	val sql = s"""Insert overwrite table $schemaName.$des_tblName select * from qualityTable"""
	spark.sql(sql) 

	val refreshCount = spark.read.format(fileFormat).table(schemaName + "." + des_tblName).count
	val after_timestamp: Long = System.currentTimeMillis()
    	auditRecord(batch_Num, src_tblName, des_tblName, refreshMode, refreshCount, before_timestamp, after_timestamp, "COMPLETED", (after_timestamp - before_timestamp)  / 1000, "")
        

}
catch{
	case e: Exception => {
         
        auditRecord(batch_Num, src_tblName, des_tblName, refreshMode, 0, System.currentTimeMillis(), 0, "FAILED", 0, e.getMessage)
        
          }
        }



      }
   
      
      

  def main(args: Array[String]): Unit = {
    
    val batch_Num = System.currentTimeMillis().toString
    val sourceSchema = "g00103"
    val sourceMasterTableName = "iqp_master_qa"
    val audit_tblName  = "spark_ingestion_audit"
    val filerKeyWord = "filter="

    
    val log = LogManager.getRootLogger
                log.setLevel(Level.WARN)

     val warehouseLocation = new File("spark-warehouse").getAbsolutePath
 
    val spark = SparkSession.builder
      .appName("business-app")
      .config("spark.some.config.option", "config-value")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()

        import spark.implicits._
   
    val auditRecordArrayBuffer: ArrayBuffer[Audit] = ArrayBuffer()
    try { 
    val custom_filter = for( arg<-args
                            if arg.startsWith(filerKeyWord)
                           )yield arg.replace("filerKeyWord","")
    val filter = if (custom_filter.isEmpty) "" else custom_filter.mkString(",").replace(filerKeyWord,"")

    val sql = s"""select schema_name ,table_name , query_string ,number_of_partitions,refresh_mode from $sourceSchema.$sourceMasterTableName
              where active_flg ='Y' ${
                  if (!filter.isEmpty)
                      s" AND $filter"
                  else ""
    }"""


    
    val dfMasterTable = spark.sql(sql)
    dfMasterTable.collect().foreach { row => auditRecordArrayBuffer +=  tblInsert(spark,batch_Num,row(0).toString, row(1).toString,row(2).toString,row.getAs[Int](3),row(4).toString,"orc") }
   } 
   catch {
          case e: Exception => {
              auditRecord(batch_Num,"NA" ,"NA","NA", 0, System.currentTimeMillis(), 0, "FAILED", 0, e.getMessage)
                      
    }
        }

    //Coverting AuditRecord collection to Dataframe to write to hive table
    finally{
    auditRecordArrayBuffer.toList.toDF.write.mode("append").saveAsTable(sourceSchema + "." + audit_tblName)
    }
  
        
        spark.stop()
  }
}