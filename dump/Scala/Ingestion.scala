import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import scala.collection.mutable.ArrayBuffer
import java.text.SimpleDateFormat
import org.apache.log4j.{Level,LogManager}
import java.io.File
import org.apache.hadoop.mapred.InvalidInputException

object Ingestion {

  //Define Schema for Audit table to track transaction details of each table ingestion
  case class AuditTableSchema(batch_number: String, source_tbl_name: String, des_tbl_name: String, refresh_mode: String, refresh_rec_count: Long,
                              refresh_start_time: String, refresh_end_time: String, refresh_status: String, refresh_runtime: Long, message: String)

  // Function to create an Intance of Audit record
  def auditRecord(batch_number: String, source_tbl_name: String, des_tbl_name: String,
                  refresh_mode: String, refresh_rec_count: Long, refresh_start_time: Long,
                  refresh_end_time: Long, refresh_status: String, refresh_runtime: Long, message: String):AuditTableSchema = {
            val dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
            new AuditTableSchema(
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

  def tblInsert(spark:SparkSession,batch_Num:String,schemaName: String, src_tblName: String, des_tblName: String, numPartition: Int, refreshMode: String,
                fileFormat: String,truncate_condition : Boolean,partition_cols_name:String ,sortby_cols_name:String): AuditTableSchema = {
          /*
        This function reads data from Hive external table to create a Spark Dataframe. And partition it across
        spark executors ( containers) for parallelism and write into Hive internal table in specified format
      */
        try {
          val before_timestamp: Long = System.currentTimeMillis()
          val df = spark.read.table(schemaName + "." + src_tblName)
          val df_partition_size = df.rdd.partitions.size
         
	  val sort_by = s"""${ if (!sortby_cols_name.isEmpty)
                      s""" order by $sortby_cols_name"""
                  else ""}"""
	
         val partition_by =s"""${ if (!partition_cols_name.isEmpty)
                      s""" PARTITION ( $partition_cols_name )"""
                  else ""}"""
                 if ( truncate_condition == false)
				 println(refreshMode.toUpperCase + " false")
                 
                 else
					 println(refreshMode.toUpperCase + " true")
					 
         val sql = if ( truncate_condition == false && refreshMode.toUpperCase == "APPEND" ) 
          s"""Insert into table $schemaName.$des_tblName $partition_by select * from internalTable $sort_by """  
          
					else
           s"""insert overwrite  table $schemaName.$des_tblName $partition_by select * from internalTable $sort_by """		  
          
                       
                
		 
          

           
        val final_partition_num = if (numPartition <= 0) df_partition_size else numPartition
        val resultWithPartit = df.coalesce(final_partition_num)
	resultWithPartit.createOrReplaceTempView("internalTable") 
        spark.sql(sql)

          
          val refreshCount = spark.read.format(fileFormat).table(schemaName + "." + des_tblName).count
          val after_timestamp: Long = System.currentTimeMillis()
          auditRecord(batch_Num, src_tblName, des_tblName, refreshMode, refreshCount, before_timestamp, after_timestamp, "COMPLETED", (after_timestamp - before_timestamp) / 1000, "")
        }
        catch {
          case e: InvalidInputException => {
            auditRecord(batch_Num, src_tblName, des_tblName, refreshMode, 0, System.currentTimeMillis(), 0, "COMPLETED", 0, "")
          }

          case e: Exception => {
            auditRecord(batch_Num, src_tblName, des_tblName, refreshMode, 0, System.currentTimeMillis(), 0, "FAILED", 0, e.getMessage)
          }
        }
      }

  def main(args: Array[String]): Unit = {
    


    val batch_Num = System.currentTimeMillis().toString
    val sourceSchema = "g00103"
    val sourceMasterTableName = "gs_iqp_master"
    val audit_tblName  = "spark_ingestion_audit"
    val filerKeyWord = "filter="
   

    val log = LogManager.getRootLogger
                log.setLevel(Level.WARN)

     val warehouseLocation = new File("spark-warehouse").getAbsolutePath
     val auditRecordArrayBuffer: ArrayBuffer[AuditTableSchema] = ArrayBuffer()
     
     val spark = SparkSession.builder
      .appName("Ingestion Framework")
      .config("spark.some.config.option", "config-value")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.orc.filterPushdown", "true")
      .config("spark.storage.StorageLevel","MEMORY_ONLY_SER") 
      .config("spark.sql.warehouse.dir", warehouseLocation)
	  .config("hive.exec.dynamic.partition","true")
	  .config("hive.exec.dynamic.partition.mode","nonstrict")
      .enableHiveSupport()
      .getOrCreate()

        import spark.implicits._
    try{
    // Memory Optimized Scala Collection to collect Audit records
    

    //Code snippet to filter out SearchExpr from Args
    val custom_filter = for( arg<-args
                            if arg.startsWith(filerKeyWord)
                           )yield arg.replace("filerKeyWord","")

    val filter = if (custom_filter.isEmpty) "" else custom_filter.mkString(",").replace(filerKeyWord,"")

    val truncFlg = args.exists{ x => x.toLowerCase.contains("truncate=true") }     

    val sql = s"""select schema_name ,source_tbl_name,des_tbl_name,numPartitions,refresh_mode,partition_cols_name,sortby_cols_name from $sourceSchema.$sourceMasterTableName
              where active_flg ='Y' ${
                  if (!filter.isEmpty)
                      s" AND $filter"
                  else ""
    }"""
    val dfMasterTable = spark.sql(sql)
    dfMasterTable.collect().foreach { row =>
     auditRecordArrayBuffer += tblInsert(spark,batch_Num,row(0).toString, row(1).toString, row(2).toString, row.getAs[Int](3), row(4).toString, "orc",truncFlg,row(5).toString,row(6).toString)
    	}
    	
    
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


