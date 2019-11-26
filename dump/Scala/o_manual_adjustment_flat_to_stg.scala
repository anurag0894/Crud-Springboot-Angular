import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.DataFrame
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import java.io.File
import org.apache.spark.storage.StorageLevel
import java.text.SimpleDateFormat
import java.util.Date
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType,DateType,TimestampType,LongType}
import org.apache.spark.sql.functions.{array, lit, map, concat_ws}
import org.apache.spark.sql.functions.explode




object o_manual_adjustment_flat_to_stg
{

def readCSV(path:String,fileSchema:StructType,isHeader:Boolean,inferSchema:Boolean) :org.apache.spark.sql.DataFrame = {
  val df =  spark.read.format("csv")
              .option("treatEmptyValuesAsNulls",true)	
              .option("ignoreLeadingWhiteSpace" ,true)
              .option("ignoreTrailingWhiteSpace" ,true)
              .option("inferSchema" ,inferSchema)
              .option("header" ,isHeader)
              .option("timestampFormat" ,"yyyy-MM-dd HH:mm:ss")	  
              .schema(fileSchema)
              .load(path)
      df
}


def colType(datatype:String) = {
   datatype.toLowerCase match  { 
  case "string" => org.apache.spark.sql.types.StringType
  case "integer" => org.apache.spark.sql.types.IntegerType
  case "date" => org.apache.spark.sql.types.DateType
  case "short" => org.apache.spark.sql.types.ShortType
  case "long" => org.apache.spark.sql.types.LongType
  case "float" => org.apache.spark.sql.types.FloatType
  case "timestamp" => org.apache.spark.sql.types.TimestampType
  case "boolean" => org.apache.spark.sql.types.BooleanType 
  case _ => org.apache.spark.sql.types.StringType
} 
 }
  val tableName = "manual_adjustment_flatfile_to_stg"
  val sConnect = new Utils.sparkConnection
  val spark = sConnect.connect(tableName)
   import spark.implicits._
def main(args: Array[String])={

val deBug = args.exists{ x => x.toLowerCase.contains("debug=true") }

      var isSuccess: Boolean = true
      var errorMessage: String = ""
      val batchNumber = spark.sparkContext.applicationId
      val beforeTimestamp: Long = System.currentTimeMillis()
      val formatter = java.text.NumberFormat.getIntegerInstance
      if (deBug) Utils.printLog(deBug,tableName + ": Debug is Enabled for batchNumber: " + batchNumber)

try {
  
  val schema_create = (arr1:Array[(String,String)]) => {
StructType(arr1.map(row => StructField(row._1,colType(row._2),true)))}

  //Code to read the CSV filelist.csv from HDFS and filtering only XLS and XML contents from the file
    val filelist_schema = StructType(Array(StructField("filename",StringType,true)))
    val filelist_path = "/tmp/manual_adjustments/XLSFiles/filelist.csv"
    val filelist_ds = readCSV(filelist_path,filelist_schema,false,false).as[String]
    val filelist_ds_filtered = filelist_ds.filter( filelist_ds("filename").endsWith("xls") || filelist_ds("filename").endsWith("xml"))
    filelist_ds_filtered.createOrReplaceTempView("mytempTable") 
    //SFTP_NEWFILE_TEST


  //Code to read the CSV indirect_backlog.txt from HDFS
    val indirectBacklog_schema = StructType(Array(StructField("filename",StringType,true)))
    val indirectBacklog_path = "/tmp/manual_adjustments/Indirect_BACKLOG.txt"
    val indirectBacklog_ds = readCSV(indirectBacklog_path,indirectBacklog_schema,false,false).as[String]  //Indirect_BACKLOG_temp

//Dataset for existing table SFTP_EXISTING_LIST
val sftp_existing_list_ds = spark.sql("select filename from g00103.sftp_existing_list").as[String].cache  //SFTP_EXISTING_LIST


val newfile_ds = filelist_ds_filtered.except(sftp_existing_list_ds)  //List of new files
newfile_ds.createOrReplaceTempView("newfile_ds") //registering it as temp table



//Append new file list to existing table sftp_existing_list only if there is new files to be added
if (newfile_ds.count >0)
  spark.sql("insert into table g00103.sftp_existing_list select filename from newfile_ds")

  

  
  
 //BACKLOG
 
//Find the number of files that needs to be appended to a tableName
val fileNameArray = indirectBacklog_ds.collect
//val fileNameArray = Array("170847938_20172809011208.csv","170962605_20170210075807.csv","171077136_20170610000000.csv")
val csvfilePath = "/tmp/manual_adjustments/CSVFiles/"
val schemaArray_backlog = Array(
("field1","String"),
("field2","String") ,
("metric_adjusted","String"), 
("week_no___fw","String"),
("backlog_update_month","string"),
("fiscal_year","string"),
("customer_name___ship_to","string"),
("transaction_type","string"),
("order_number","string"),
("purchase_order_no","string"),
("management_entity","string"),
("formula_driven_tier5","string"),
("product_class","string"),
("country_name___ship_to","string"),
("us___canada_only__country_state___ship_to","string"),
("formula_driven_region","string"),
("formula_driven_sub_region","string"),
("us_total_backlog","string"),
("month_1","string"),
("month_2","string"),
("month_3","string"),
("month_4","string"),
("month_5","string"),
("month_6","string"),
("month_7","string"),
("month_8","string"),
("month_9","string"),
("month_10","string"),
("month_11","string"),
("month_12","string"),
("month_13","string"),
("month_14","string"),
("month_15","string"),
("month_16","string"),
("month_17","string"),
("month_18","string"),
("formula_driven_remaining_after_18_months_split","string"),
("backlog_end_date","string"),
("local_currency","string"),
("local_order_value","string"),
("mor_month","string"),
("exchange_rate___local_currency_usd","string"),
("customer_number___ship_to","string"),
("end_customer_name","string"),
("channel","string"),
("customer_sector","string"),
("sales_person","string"),
("field48","string"),
("metric_adjusted1","string"),
("week_no___fw1","string"),
("fiscal_year1","string"),
("customer_name___ship_to1","string"),
("management_entity1","string"),
("formula_driven_tier51","string"),
("product_class1","string"),
("country_name___ship_to1","string"),
("us___canada_only__country_state___ship_to1","string"),
("formula_driven_region1","string"),
("formula_driven_sub_region1","string"),
("us_sales_value","string"),
("invoice_number","string"),
("order_number1","string"),
("sales_account_no","string"),
("formula_driven___________________transaction_type","string"),
("channel1","string"),
("adjustment_type","string"),
("us_cm_value","string"),
("local_currency1","string"),
("local_order_value1","string"),
("mor_month1","string"),
("exchange_rate___local_currency_usd1","string"),
("customer_number___ship_to1","string"),
("end_customer_name1","string"),
("customer_sector1","string"),
("sales_person1","string"),
("reason_for_manual_adjustment","string"))

val schema_backlog = schema_create(schemaArray_backlog)


for ( fileName <-fileNameArray ){
    val file_path = csvfilePath + fileName
    val temp_df = readCSV(file_path,schema_backlog,true,true)
    temp_df.createOrReplaceTempView("temp_df")
    val sql = s""" insert into g00103.BACKLOG_STAGE_FLAT_FILE select temp_df.*,'$fileName' from temp_df """
    spark.sql(sql)
}


spark.sql("""insert overwrite table g00103.BACKLOG_STAGE select
Metric_Adjusted,
Week_No___FW,
Backlog_Update_Month,
Fiscal_Year,
customer_name___ship_to,
Transaction_Type,
Order_Number,
Purchase_Order_No,
Management_Entity,
Formula_Driven_Tier5,
Product_Class,
us___canada_only__country_state___ship_to,
Formula_driven_Region,
Formula_driven_Sub_region,
US_Total_Backlog,
Month_1,
Month_2,
Month_3,
Month_4,
Month_5,
Month_6,
Month_7,
Month_8,
Month_9,
Month_10,
Month_11,
Month_12,
Month_13,
Month_14,
Month_15,
Month_16,
Month_17,
Month_18,
Formula_driven_Remaining_after_18_months_Split,
to_date(from_unixtime(unix_timestamp(concat(substr(backlog_end_date,9,2),'-',substr(backlog_end_date,5,3),'-',substr(backlog_end_date,25,4)),'dd-MMM-yyyy'))),
Local_Currency,
Local_Order_Value,
MOR_Month,
Exchange_Rate___Local_Currency_USD,
Customer_Number___Ship_To,
End_Customer_Name,
Channel,
Customer_Sector,
Sales_Person,
NULL as REASON_FOR_MANUAL_ADJUSTMENT,
country_name___ship_to,
NULL as Process_date,
NULL as CUSTOMER_REQUEST_DATE,
CurrentlyProcessedFileName,
Null as ISPROCESSED
from
g00103.BACKLOG_STAGE_FLAT_FILE
where
Metric_Adjusted is not null and upper(Metric_Adjusted)='BACKLOG'
""")
spark.sql("truncate table g00103.BACKLOG_STAGE_FLAT_FILE")

//SALES


 //Code to read the CSV Indirect_SALES.txt from HDFS
    val indirectSales_schema = StructType(Array(StructField("filename",StringType,true)))
    val indirectSales_path = "/tmp/manual_adjustments/Indirect_SALES.txt"
    val indirectSales_ds = readCSV(indirectSales_path,indirectSales_schema,false,false).as[String]  
	
val fileNameArray_sales = indirectSales_ds.collect
//val fileNameArray = Array("170847938_20172809011208.csv","170962605_20170210075807.csv","171077136_20170610000000.csv")

for ( fileName <-fileNameArray_sales ){
    val file_path = csvfilePath + fileName
    val temp_df = readCSV(file_path,schema_backlog,true,true)
    temp_df.createOrReplaceTempView("temp_df")
    val sql = s""" insert into g00103.BACKLOG_STAGE_FLAT_FILE select temp_df.*,'$fileName' from temp_df """
    spark.sql(sql)
}


spark.sql("""insert into table g00103.SALES_STAGE select
FIELD48,
Metric_Adjusted1,
week_no___fw1,
fiscal_year1,
customer_name___ship_to1,
management_entity1,
formula_driven_tier51,
product_class1,
country_name___ship_to1,
us___canada_only__country_state___ship_to1,
formula_driven_region1,
formula_driven_sub_region1,
us_sales_value,
invoice_number,
order_number1,
sales_account_no,
formula_driven___________________transaction_type,
channel1,
adjustment_type,
us_cm_value,
local_currency1,
local_order_value1,
mor_month1,
exchange_rate___local_currency_usd1,
customer_number___ship_to1,
end_customer_name1,
customer_sector1,
sales_person1,
currentlyprocessedfilename
from
g00103.BACKLOG_STAGE_FLAT_FILE
where UPPER(FIELD48)='SALES'
""")
spark.sql("""insert into table g00103.SALES_STAGE select
metric_adjusted1,
week_no___fw1,
fiscal_year1,
customer_name___ship_to1,
management_entity1,
formula_driven_tier51,
product_class1,
country_name___ship_to1,
us___canada_only__country_state___ship_to1,
formula_driven_region1,
formula_driven_sub_region1,
us_sales_value,
invoice_number,
order_number1,
sales_account_no,
formula_driven___________________transaction_type,
channel1,
adjustment_type,
us_cm_value,
local_currency1,
local_order_value1,
mor_month1,
exchange_rate___local_currency_usd1,
customer_number___ship_to1,
end_customer_name1,
customer_sector1,
sales_person1,
reason_for_manual_adjustment,
currentlyprocessedfilename
from
g00103.BACKLOG_STAGE_FLAT_FILE
where upper(Metric_Adjusted1)='SALES'
""")

spark.sql("truncate table g00103.BACKLOG_STAGE_FLAT_FILE")


  //Code to read the CSV Indirect_ORDERS.txt from HDFS
    val indirectOrders_schema = StructType(Array(StructField("filename",StringType,true)))
    val indirectOrders_path = "/tmp/manual_adjustments/Indirect_ORDERS.txt"
    val indirectOrders_ds = readCSV(indirectBacklog_path,indirectBacklog_schema,false,false).as[String]  //Indirect_BACKLOG_temp


 //ORDERS
 
//Find the number of files that needs to be appended to a tableName
val fileNameArray_orders = indirectOrders_ds.collect

val schemaArray_orders= Array(
("field1","string"),
("metric_adjusted","string"),
("week_no_fw","string"),
("fiscal_year","string"),
("customer_name_ship_to","string"),
("us_order_value","string"),
("order_number","string"),
("purchase_order_no","string"),
("order_booked_date","string"),
("management_entity","string"),
("formula_driven_tier5","string"),
("product_class","string"),
("transaction_type","string"),
("country_name_ship_to","string"),
("us_canada_only_country_state_ship_to","string"),
("channel","string"),
("adjustment_type","string"),
("customer_number_ship_to","string"),
("end_customer_name","string"),
("local_currency","string"),
("local_order_value","string"),
("mor_month","string"),
("exchange_rate_local_currency_usd","string"),
("customer_request_date","string"),
("t_d_only_start_date","string"),
("t_d_only_end_date","string"),
("customer_sector","string"),
("sales_person","string"),
("reason_for_manual_adjustment","string"),
("field30","string"),
("field31","string"),
("field32","string"),
("field33","string"),
("field34","string"),
("field35","string"),
("field36","string"),
("currentlyprocessedfilename","string"))

val schema_orders= schema_create(schemaArray_orders)



for ( fileName <-fileNameArray_orders ){
    val file_path = csvfilePath + fileName
    val temp_df = readCSV(file_path,schema_orders,true,true)
    temp_df.createOrReplaceTempView("temp_df")
    val sql = s""" insert into g00103.orders_stage_flat_file select temp_df.*,'$fileName' from temp_df """
    spark.sql(sql)
}

val result = spark.sql("""insert overwrite table g00103.ORDERS_STAGE select Metric_Adjusted as Metric_Adjusted,
Customer_Name_Ship_To,
cast(US_Order_Value as double),
Order_Number,
Purchase_Order_No,
Order_Booked_Date,
Management_Entity,
Formula_Driven_Tier5,
Product_Class,
Transaction_Type,
Country_Name_Ship_To,
US_Canada_only_Country_State_Ship_To,
Channel,
Adjustment_Type,
Customer_Number_Ship_To,
End_Customer_Name,
Local_Currency,
cast(Local_Order_Value as double),
MOR_Month,
cast(Exchange_Rate_Local_Currency_USD as double),
Customer_Request_Date,
T_D_only_Start_Date,
T_D_only_End_Date,
Customer_Sector,
Sales_Person,
Reason_for_manual_adjustment,
current_timestamp as PROCESS_DATE,
CurrentlyProcessedFileName,
cast(Week_No_FW as bigint),
cast(Fiscal_Year as bigint)
from 
g00103.orders_STAGE_FLAT_FILE
where 
Metric_Adjusted is not null  AND upper(Metric_Adjusted)='ORDERS'
    and cast(Week_No_FW as bigint) BETWEEN 1 AND 53
and Order_Number is not null
AND LENGTH(Fiscal_Year)=4
    AND cast(Fiscal_Year as bigint) BETWEEN '1900' AND '9999'
    AND cast(US_Order_Value  as double) is not null
AND LENGTH(Customer_Request_Date) >=8
AND LENGTH(Order_Booked_Date) >=8""")

spark.sql("truncate table g00103.orders_stage_flat_file")





if (deBug) Utils.printLog(deBug, tableName + ": Processing Completed ...")

} catch {
  case e: Throwable => {
          isSuccess=false
          errorMessage=e.getMessage()
                        }
        }
  finally {
    val status = if (isSuccess) "SUCCESS" else "FAILED"
    if (deBug) Utils.printLog(deBug, tableName + ": Writing to audit table with status: " + status)
    Utils.auditRecord(spark, Utils.AuditRecord(batchNumber, tableName, beforeTimestamp, status, errorMessage))
    spark.stop
      }
  }
}
