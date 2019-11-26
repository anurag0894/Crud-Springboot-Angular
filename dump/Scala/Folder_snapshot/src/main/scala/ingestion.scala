import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import scala.collection.mutable.ArrayBuffer
import java.text.SimpleDateFormat
import org.apache.log4j.{Level,LogManager}
import java.io.File

object ingestion {

    //Global variables
    var batchNum: String = "N/A#"
    var finalsql:String = "TO BE CREATED"
    //Define Schema for Audit table to track transaction details of each table ingestion
    case class AuditTableSchema(batch_number: String, source_tbl_name: String, des_tbl_name: String, refresh_mode: String, refresh_rec_count: Long,refresh_start_time: String, refresh_end_time: String, refresh_status: String, refresh_runtime: Long, message: String)

    //Define Schema for master ingestion table
    case class IngestionSchema(db_name :String ,source_tbl_name :String ,des_tbl_name :String ,functional_grp_name :String ,group_number :Integer,active_flg:String,num_parallel_task:Integer ,refresh_mode :String ,partition_cols_name :String ,sortby_cols_name :String ,filterby_col_name :String ,tranform_flg :String ,tranform_sql :String ,processing_order :Integer ,source_system_name :String,source_system_id :String, file_format :String)

  // Function to create an Intance of Audit record
  def auditRecord(batch_number: String, source_tbl_name: String, des_tbl_name: String,
                  refresh_mode: String, refresh_rec_count: Long, refresh_start_time: Long,
                  refresh_end_time: Long, refresh_status: String, refresh_runtime: Long, message: String):AuditTableSchema = {
            val dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss:mmm")
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

    def tblInsert(spark:SparkSession,ingestionRow:IngestionSchema ): AuditTableSchema = {

            val dbName = ingestionRow.db_name
            val source_tbl_name = ingestionRow.source_tbl_name
            val des_tbl_name = ingestionRow.des_tbl_name
            val num_parallel_task = ingestionRow.num_parallel_task
            val refresh_mode = ingestionRow.refresh_mode
            val partition_cols_name = ingestionRow.partition_cols_name
            val sortby_cols_name = ingestionRow.sortby_cols_name
            val filterby_col_name = ingestionRow.filterby_col_name
            val tranform_flg = ingestionRow.tranform_flg
            val tranform_sql = ingestionRow.tranform_sql
            val source_system_name = ingestionRow.source_system_name
            val source_system_id = ingestionRow.source_system_id
            val fileformat =  Option(ingestionRow.file_format).getOrElse("orc")

            val before_timestamp: Long = System.currentTimeMillis()
            var status : String = "UNKNOW"
            var message : String = ""
            var dest_tbl_count:Long = -1

            try {
              val sort_by = s""" ${ if (!sortby_cols_name.isEmpty)  s" order by $sortby_cols_name " else "" }"""
              val partition_by = s""" ${ if (!partition_cols_name.isEmpty) s" PARTITION ( $partition_cols_name ) " else "" }"""
              val filter_by = filterby_col_name
              val select =  s""" ${ if (!tranform_sql.isEmpty && Option(tranform_flg).getOrElse("N") == "Y" ) s"$tranform_sql " else "select * " } """
             
              //Create dataframe from external table and find out count of records to be inserted
              val df_src_tbl = spark.read.table(dbName + "." + source_tbl_name)
              val df_src_tbl_filter = if (filter_by.isEmpty) df_src_tbl else  df_src_tbl.filter(filter_by)
              val final_partition_num:Integer = if (num_parallel_task <= 0) df_src_tbl_filter.rdd.partitions.size else num_parallel_task
              val df_src_tbl_filter_final = df_src_tbl_filter.coalesce(final_partition_num)

              df_src_tbl_filter_final.createOrReplaceTempView("spark_tmp_tbl")
              val df_src_final =  spark.sql(s""" $select from spark_tmp_tbl  """)
              val df_source_tbl_count:Long = scala.util.Try(df_src_final.count).getOrElse(-1)

              //Further processing only if there are records in given table
               if (  df_source_tbl_count > 0 )
              {
                  //Finalizing insert mode
                  val insert_mode = refresh_mode.toUpperCase match {
                      case "APPEND" => "INSERT INTO"
                      case "ADHOC REFRESH" => "INSERT OVERWRITE"
                      case "FULL LOAD" => "INSERT OVERWRITE"
                      case _ => "UNKNOWN"
                    }
                    if( insert_mode != "UNKNOWN" )
                    {
                          //Setting load date to precision of millisecond in format YYYY-MM-DD HH:MM:SS.MMM
                          val loadTimestampFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.mmm")
                          val currentTimestamp = System.currentTimeMillis()
                          val loadTimestamp = loadTimestampFormat.format(currentTimestamp)

                        if ( refresh_mode == "ADHOC REFRESH" )
                          {
                           finalsql = s""" $insert_mode TABLE $dbName.$des_tbl_name $partition_by select * FROM spark_tmp_tbl ${ if(!filter_by.isEmpty)s" WHERE $filter_by" else ""}  $sort_by """
                           }
                           else
                           {
                           finalsql = s""" $insert_mode TABLE $dbName.$des_tbl_name $partition_by $select  FROM spark_tmp_tbl ${ if(!filter_by.isEmpty)s" WHERE $filter_by" else ""}  $sort_by """
                           }
                          //Run the SQL to insert the rows
                           
                            spark.sql(finalsql)
                           dest_tbl_count = refresh_mode.toUpperCase match {
                            case "ADHOC REFRESH" => spark.read.format(fileformat).table(dbName + "." + des_tbl_name).count
                            case "FULL LOAD" => spark.read.format(fileformat).table(dbName + "." + des_tbl_name).count
                         //   case "APPEND" => spark.read.format(fileformat).table(dbName + "." + des_tbl_name).filter(s""" created_date = '$loadTimestamp' """).count
                            case _ => -1
                        }
                        status = if ( dest_tbl_count == df_source_tbl_count) "SUCCESS" else if(dest_tbl_count == -1 ) "NA" else "COUNT-MISMATCH"
                        message = if ( dest_tbl_count == df_source_tbl_count) finalsql else if(dest_tbl_count == -1 ) "NA" else s"Src_Tbl Count=$df_source_tbl_count and dest_tbl_count=$dest_tbl_count"
                    }
            }
            else if (df_source_tbl_count == 0) {
			if(refresh_mode.toUpperCase == "FULL LOAD") { finalsql = s""" TRUNCATE  TABLE $dbName.$des_tbl_name"""  
             spark.sql(	finalsql)		}
              status = "SKIP"
              message = "No Record to be inserted"
              dest_tbl_count = 0
            }
            else  {
			       
                  status = "INVALID"
                  message = "Table Not Found"
                  dest_tbl_count = -1
              }
            // add entry in aduit table
            val after_timestamp = System.currentTimeMillis()
            auditRecord(batchNum, source_tbl_name, des_tbl_name, refresh_mode, dest_tbl_count, before_timestamp, after_timestamp, status, scala.util.Try((after_timestamp - before_timestamp) / 1000).getOrElse(0), finalsql)
        }
        catch {
          case e: Exception => {
            auditRecord(batchNum, source_tbl_name, des_tbl_name, refresh_mode, 0, System.currentTimeMillis(), 0, "FAILED", 0, e.getMessage)
          }
        }
      }

  def main(args: Array[String]): Unit = {

    val audit_tblName  = "spark_ingestion_audit"          //Audit table
    val sh_name  = "g00103"          //Audit table schema
    
    val auditRecordArrayBuffer: ArrayBuffer[AuditTableSchema] = ArrayBuffer()
    //var db_name: String = ""
    val spark = Utils.connectnew("Ingestion Framework")
    batchNum = spark.sparkContext.applicationId
    import spark.implicits._
	spark.sparkContext.setLogLevel("Warn")

    try{

        //val args = Array("dbName=g00103","masterTable=gs_iqp_master","filter= source_tbl_name = '%FF_IN_MATERIAL_TYPE_SAPNA__x%'")
        //  val input_fields = Seq("debug","truncate","dbName","masterTable","filter","runId")
        val input_fields = Seq("debug","truncate","dbName","masterTable","runId","filter")

          //Code to retrive input argument values to a Map and varaible

          val args_map = Utils.filterArgumentvalues(args,input_fields,"=")
          val deBug = args_map.getOrElse("debug",false).toString.toBoolean
          val truncFlg = args_map.getOrElse("truncate","N").toString
          val filter = args_map.getOrElse("filter","").toString
          val db_name = args_map.getOrElse("dbName","g00103").toString
          val masterTable = args_map.getOrElse("masterTable","gs_iqp_master_final").toString
          val runId =  args_map.getOrElse("runId","UNKNOWN").toString
          batchNum = batchNum +"#" + runId

          //Generating SQL to query master table to obtain metadata information for the jobs
          val sql = s""" select * from $db_name.$masterTable where active_flg ='Y' ${ if(!filter.isEmpty)s" AND $filter" else ""} order by coalesce(processing_order,9999999999) desc"""
          val dfMasterTable = spark.sql(sql).as[IngestionSchema]
          dfMasterTable.collect().foreach { ingestionRow => auditRecordArrayBuffer += tblInsert(spark,ingestionRow)}
    }
catch {
          case e: Exception => {
              auditRecord(batchNum,"NA" ,"NA","NA", 0, System.currentTimeMillis(), 0, "FAIL ", 0, e.getMessage)
    }
        }
    //Updating Audit table with logs
    finally{
        auditRecordArrayBuffer.toList.toDF.write.mode("append").saveAsTable(sh_name + "." + audit_tblName)
      //spark.catalog.dropTempView("spark_tmp_tbl")
      spark.stop()
    }
 }
}
