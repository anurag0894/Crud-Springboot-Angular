import java.time.LocalDate 
import java.time.format.DateTimeFormatter
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.DataFrame
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import java.io.File
import org.apache.spark.storage.StorageLevel
import java.text.SimpleDateFormat
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.functions.{array, lit, map, concat_ws}



object o_iqp_sc_adjustments_stg
{




  val tableName = "g00103.iqp_sc_adjustments_stg"
  val sConnect = new Utils.sparkConnection
  val spark = SparkSession.builder
      .appName(tableName)
      .config("spark.some.config.option", "config-value")
//     .config("spark.sql.warehouse.dir", warehouseLocation)
//      .config("orc.compress","ZLIB")
      .enableHiveSupport()
      .getOrCreate()
      spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      spark.conf.set("spark.kryo.registrationRequired", "true")
      spark.conf.set("spark.scheduler.mode", "FAIR")
      spark.conf.set("spark.sql.orc.filterPushdown", "true")
      spark.conf.set("spark.storage.StorageLevel","MEMORY_ONLY_SER")
      spark.conf.set("spark.kryo.classesToRegister", "AuditTableSchema")
	  spark.conf.set("spark.sql.autoBroadcastJoinThreshold",52428800)
	  spark.conf.set("spark.sql.tungsten.enabled", "true")


def main(args: Array[String])={
  
val deBug = args.exists{ x => x.toLowerCase.contains("debug=true") }

      var isSuccess: Boolean = true
      var errorMessage: String = ""
      val batchNumber = spark.sparkContext.applicationId
      val beforeTimestamp: Long = System.currentTimeMillis()
      val formatter = java.text.NumberFormat.getIntegerInstance
      if (deBug) Utils.printLog(deBug,tableName + ": Debug is Enabled for batchNumber: " + batchNumber)
	  
try {
spark.conf.set("spark.kryo.classesToRegister", "AuditTableSchema")
spark.conf.set("spark.rdd.compress", "true")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold",52428800)
spark.conf.set("spark.sql.tungsten.enabled", "true")
spark.sql("set spark.sql.shuffle.partitions=10")



//SC BACKLOG
	   		   
val sc_backlog_df=spark.sql("""select metric_adjusted,
week_no_fw,
backlog_update_month,
fiscal_year,
customer_name_ship_to,
transaction_type,
order_number,
purchase_order_no,
management_entity,
tier5,
productclass,
country_name_ship_to,
us_canada_country_state,
region,
sub_region,
usd_total_backlog,
 month1,
month2,
 month3,
 month4,
 month5,
 month6,
 month7,
month8,
month9,
 month10,
 month11,
 month12,
month13,
 month14,
month15,
 month16,
 month17,
 month18,
remaining_after_18months_split,
backlog_end_date,
local_currency,
local_order_value,
mor_month,
exchange_rate,
customer_number_ship_to,
end_customer_name,
channel,
customer_sector,
sales_person,
file_name
from 
g00103.BACKLOG_STAGE""")

val udf_zip = udf((col1:Seq[String] ,col2:Seq[Int]) => col1.zip(col2))
val colId = Seq( 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18)
val sc_backlog_exploded = sc_backlog_df.withColumn("temp", explode ( udf_zip(array("month1","month2","month3","month4","month5","month6","month7","month8","month9","month10","month11","month12","month13","month14","month15","month16","month17","month18" ) ,array((colId.map(lit)):_* ) ) ) )
val sc_backlog_exploded_df = sc_backlog_exploded.withColumn("net_amount", sc_backlog_exploded("temp._1") ).withColumn("key_code",sc_backlog_exploded("temp._2") ).drop("temp" )


sc_backlog_exploded_df.createOrReplaceTempView("sc_backlog")
spark.sql("cache table sc_backlog")


spark.sql("""insert into g00103.IQP_SC_ADJUSTMENTS_STG_BACKLOG
select metric_adjusted,
week_no_fw,
backlog_update_month,
sc_backlog.fiscal_year,
customer_name_ship_to,
transaction_type,
sc_backlog.order_number,
purchase_order_no,
management_entity,
tier5,
productclass,
us_canada_country_state,
region,
sub_region,
usd_total_backlog,
net_amount,
NULL,
NULL,
NULL,
NULL,
NULL,
NULL,
NULL,
NULL,
NULL,
NULL,
NULL,
NULL,
NULL,
NULL,
NULL,
NULL,
NULL,
remaining_after_18months_split,
backlog_end_date,
local_currency,
local_order_value,
mor_month,
exchange_rate,
customer_number_ship_to,
end_customer_name,
channel,
customer_sector,
sales_person,
'OTHERS' as REASON_FOR_MANUAL_ADJUSTMENT,
country_name_ship_to,
date_lkp.DATE_VALUE as PROCESS_DATE,
month_lkp.DATE_VALUE as CUSTOMER_REQUEST_DATE,
sc_backlog.file_name,
0 as ISPROCESSED,
current_timestamp
from sc_backlog left join
(SELECT min(IQP_DATE_DIM.DATE_VALUE) as DATE_VALUE, 
IQP_DATE_DIM.FISCAL_WEEK as FISCAL_WEEK,
 IQP_DATE_DIM.FISCAL_YEAR as FISCAL_YEAR 
 FROM g00103.IQP_DATE_DIM IQP_DATE_DIM
 group by IQP_DATE_DIM.FISCAL_WEEK,IQP_DATE_DIM.FISCAL_YEAR)date_lkp on(date_lkp.FISCAL_WEEK=sc_backlog.week_no_fw and date_lkp.FISCAL_YEAR=sc_backlog.fiscal_year) left join
 
 (SELECT FISCAL_MONTH as FISCAL_MONTH ,
 MAX(DATE_VALUE) as DATE_VALUE 
 FROM  g00103.IQP_DATE_DIM IQP_DATE_DIM  group by FISCAL_MONTH)month_lkp on(month_lkp.FISCAL_MONTH=
                                                                            concat(year(add_months(date_lkp.DATE_VALUE,key_code)),
																			(case 
																			   when month(add_months(date_lkp.DATE_VALUE,key_code)) <10 then concat('0',month(add_months(date_lkp.DATE_VALUE,key_code)))
                           														else month(add_months(date_lkp.DATE_VALUE,key_code))				  
																				end))) left join
(select max(order_number) as order_number ,
file_name as FILE_NAME 
from g00103.IQP_SC_ADJUSTMENTS_STG_BACKLOG
group by file_name )target_lkp on(target_lkp.FILE_NAME=sc_backlog.file_name)																																					
where (sc_backlog.net_amount is not  null or sc_backlog.net_amount<> 0 ) and target_lkp.order_number is  null""")



//SC_BACKLOG MULTIPLICATION SQL PROCEDURE

val generateSeq = (counter: Int) =>  {
  if(counter == 0) Array(1) else  (1 to counter).toArray
}

val multiplication_df=spark.sql("""SELECT a.*,datediff(backlog_end_date,process_date) as counter FROM g00103.IQP_SC_ADJUSTMENTS_STG_BACKLOG a WHERE RECORD_CREATION_DATE 
=(SELECT  MAX(RECORD_CREATION_DATE) FROM g00103.IQP_SC_ADJUSTMENTS_STG_BACKLOG)""")

val udf_generateSeq = udf(generateSeq)

val key_code_df= multiplication_df.withColumn("exploded_col", explode(udf_generateSeq(multiplication_df("counter"))))

val multiplicated_df=key_code_df.withColumn("metric_adjusted",key_code_df("metric_adjusted") * key_code_df("exploded_col"))

multiplicated_df.createOrReplaceTempView("multiplicated_vw") 
spark.sql("cache table multiplicated_vw")
			 
spark.sql(s"""insert into g00103.IQP_SC_ADJUSTMENTS_STG_BACKLOG 
select 
metric_adjusted,
week_no_fw,
backlog_update_month,
fiscal_year,
customer_name_ship_to,
transaction_type,
order_number,
purchase_order_no,
management_entity,
tier5,
productclass,
us_canada_country_state,
region,
sub_region,
usd_total_backlog,
month1,
month2,
month3,
month4,
month5,
month6,
month7,
month8,
month9,
month10,
month11,
month12,
month13,
month14,
month15,
month16,
month17,
month18,
remaining_after_18months_split,
backlog_end_date,
local_currency,
local_order_value,
mor_month,
exchange_rate,
customer_number_ship_to,
end_customer_name,
channel,
customer_sector,
sales_person,
reason_for_manual_adjustment,
country_name_ship_to,
date_add(process_date,exploded_col),
customer_request_date,
file_name,
isprocessed,
record_creation_date
from 
multiplicated_vw
where counter<>0
""")


//SC SALES

spark.sql("insert into  g00103.IQP_SC_ADJUSTMENTS_STG_SALES select * from g00103.sales_stage")
spark.sql("truncate table g00103.sales_stage")

//SC ORDERS



val all_auto_reversing_df = spark.sql("""select 
a.Metric_Adjusted,
a.CUSTOMER_NAME_SHIP_TO,
a.US_Order_Value,
a.Order_Number,
a.Purchase_Order_No,
a.Order_Booked_Date,
a.Management_Entity,
a.Formula_Driven_Tier5,
a.Product_Class,
a.Transaction_Type,
a.Country_Name_Ship_To,
a.USCANADA_COUNTRY_STATE_SHIP_TO,
a.Channel,
a.Adjustment_Type,
a.Customer_Number_Ship_To,
a.End_Customer_Name,
a.Local_Currency,
a.Local_Order_Value,
a.MOR_Month,
a.EXCHNG_RATE_LOCAL_CURR_USD,
case when a.Customer_Request_Date is null then to_date('1900-01-01')
else a.Customer_Request_Date end,
NULL,
NULL,
a.Customer_Sector,
a.Sales_Person,
a.Reason_for_manual_adjustment,
b.DATE_VALUE,
a.FILE_NAME,
a.FISCAL_YEAR,
a.Week_No_FW,
NULL
from
g00103.ORDERS_STAGE a
left join (SELECT min(IQP_DATE_DIM.DATE_VALUE) as DATE_VALUE, 
IQP_DATE_DIM.FISCAL_WEEK as FISCAL_WEEK, 
IQP_DATE_DIM.FISCAL_YEAR as FISCAL_YEAR,
max(IQP_DATE_DIM.FISCAL_QUARTER) as FISCAL_QUARTER
 FROM g00103.IQP_DATE_DIM
group by FISCAL_WEEK,FISCAL_YEAR) b on (a.Week_No_FW = b.FISCAL_WEEK and a.FISCAL_YEAR = b.FISCAL_YEAR)""")



val Auto_reversing_df = spark.sql("""select 
a.Metric_Adjusted,
a.CUSTOMER_NAME_SHIP_TO,
-1*a.US_Order_Value,
a.Order_Number,
a.Purchase_Order_No,
a.Order_Booked_Date,
a.Management_Entity,
a.Formula_Driven_Tier5,
a.Product_Class,
a.Transaction_Type,
a.Country_Name_Ship_To,
a.USCANADA_COUNTRY_STATE_SHIP_TO,
a.Channel,
'AT',
a.Customer_Number_Ship_To,
a.End_Customer_Name,
a.Local_Currency,
a.Local_Order_Value,
a.MOR_Month,
a.EXCHNG_RATE_LOCAL_CURR_USD,
case when a.Customer_Request_Date is null then to_date('1900-01-01')
else a.Customer_Request_Date end,
NULL,
NULL,
a.Customer_Sector,
a.Sales_Person,
a.Reason_for_manual_adjustment,
c.DATE_VALUE,
a.FILE_NAME,
c.FISCAL_YEAR,
c.FISCAL_WEEK,
NULL
from
g00103.ORDERS_STAGE a
left join (SELECT max(IQP_DATE_DIM.DATE_VALUE) as DATE_VALUE, 
IQP_DATE_DIM.FISCAL_WEEK as FISCAL_WEEK, 
IQP_DATE_DIM.FISCAL_YEAR as FISCAL_YEAR,
max(IQP_DATE_DIM.FISCAL_QUARTER) as FISCAL_QUARTER
 FROM g00103.IQP_DATE_DIM
group by FISCAL_WEEK,FISCAL_YEAR) b on (a.Week_No_FW = b.FISCAL_WEEK and a.FISCAL_YEAR = b.FISCAL_YEAR)
left join (SELECT
min(IQP_DATE_DIM.DATE_KEY) as DATE_KEY, 
min(IQP_DATE_DIM.DATE_VALUE) as DATE_VALUE, 
min(IQP_DATE_DIM.FISCAL_YEAR) as FISCAL_YEAR, 
min(IQP_DATE_DIM.FISCAL_YEAR_NAME) as FISCAL_YEAR_NAME, 
IQP_DATE_DIM.FISCAL_QUARTER as FISCAL_QUARTER, 
min(IQP_DATE_DIM.FISCAL_WEEK) as FISCAL_WEEK 
 FROM g00103.IQP_DATE_DIM IQP_DATE_DIM
group by FISCAL_QUARTER) c on (case when cast(SUBSTR(b.FISCAL_QUARTER,5) as Int)<4 then cast(concat(b.FISCAL_YEAR,cast (SUBSTR(b.FISCAL_QUARTER,5) as Int)+1) as Int)
else cast(concat(b.FISCAL_YEAR+1,'1') as Int) end = c.FISCAL_QUARTER)
where a.Adjustment_Type = 'Auto-reversing'""")

val sc_orders_stage_df = all_auto_reversing_df.union(Auto_reversing_df )
sc_orders_stage_df.createOrReplaceTempView("sc_orders_stage_vw")

spark.sql("""insert into g00103.IQP_SC_ADJUSTMENTS_STG_ORDERS select * from sc_orders_stage_vw""")


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

