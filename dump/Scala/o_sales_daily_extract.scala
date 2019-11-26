import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.DataFrame
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import java.io.File
import org.apache.spark.storage.StorageLevel
import java.text.SimpleDateFormat
import org.apache.hadoop.fs._;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType,DateType,TimestampType,LongType}
import org.apache.spark.sql.functions.{array, lit, map, concat_ws,col}
import org.apache.spark.sql.functions.explode

object o_sales_daily_extract
{
  val tableName = "g00103.sales_daily_extract"
 val sConnect = new Utils.sparkConnection
// val spark = sConnect.connect(tableName)
  
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

def main(args: Array[String]):Unit={
val deBug = args.exists{ x => x.toLowerCase.contains("debug=true") }

      var isSuccess: Boolean = true
      var errorMessage: String = ""
      val batchNumber = spark.sparkContext.applicationId
      val beforeTimestamp: Long = System.currentTimeMillis()
      val formatter = java.text.NumberFormat.getIntegerInstance
    //  if (deBug) Utils.printLog(deBug,tableName + ": Debug is Enabled for batchNumber: " + batchNumber)
                  
try {
val input_fields = Seq("debug","truncate","dbName","masterTable","runId","filename","filter1","filter2","filter3","filter4")

val args_map = Utils.filterArgumentvalues(args,input_fields,"=")
          val deBug = args_map.getOrElse("debug",false).toString.toBoolean
          val truncFlg = args_map.getOrElse("truncate","N").toString
          val filter1 = args_map.getOrElse("filter1","").toString
          val db_name = args_map.getOrElse("dbName","g00103").toString
          val masterTable = args_map.getOrElse("masterTable","gs_iqp_master_final").toString
          val runId =  args_map.getOrElse("runId","UNKNOWN").toString
		  val filter2 = args_map.getOrElse("filter2","").toString
		  val filter3 = args_map.getOrElse("filter3","").toString
		  val filter4 = args_map.getOrElse("filter4","").toString
		  val filename = args_map.getOrElse("filename","").toString

val sql = s"""SELECT  substr(IOBF.PROCESSDATE,1,10) as `#ProcessDate`,
  trim(IOBF.CUSTOMERNUMBER) as CustomerNumber,
  trim(regexp_replace(IOBF.CUSTOMERNAME,'\"','')) CustomerName,
  trim(IOBF.BILL_TO_CLASSIFY) as CustomerGroup,
  trim(IBHD.TIER3) PandL_Tier3,
	nvl (
  CASE
    WHEN ibhd.tier2 = 'GRID AUTOMATION'
    AND ibhd.tier3  = regexp_replace('PROTECTION _ CONTROL','_','&')
    THEN trim(IOBF.PRODUCT_PLATFORM)
    WHEN UPPER(IBHD.TIER2)='GRID AUTOMATION'
    AND IBHD.TIER3='M&D'
    THEN trim(IOBF.PRODUCT_LINE)
    ELSE trim(IBHD.PNL)
  END,'TBD') Pandl,
  trim(IBHD.SUBPNL) as SubPandL,
  RTRIM(LTRIM(UPPER(case When IOBF.CREATED_BY in ('iqp_sales_agg_erp') and IOBF.warehouse_name in ('IO Multilin CA', 'IO Multilin PR', 'IO Multilin US')
THEN NVL(IOBF.SOLDTO_COUNTRY,NVL(IOBF.SHIPTO_COUNTRY,IOBF.BILL_TO_COUNTRY) )   
else NVL(IOBF.shipto_country,IOBF.Bill_to_Country)  end))) as CountryName,
  trim(iobf.region)     AS WorldPole,
  trim(IOBF.SUB_REGION) AS WorldSubPole,
   IDD.FISCAL_WEEK as FiscalWeek,
  IDD.FISCAL_QUARTER as FiscalQuarter,
  IDD.CALENDAR_YEAR as CurrentYear,
    IOBF.REQUISITIONNUMBER as RequisitionNumber,
  IOBF.NETAMOUNT NetAmount,
CASE 
WHEN NVL(WAREHOUSE_NAME,'XXX') IN ('IO Multilin US','IO Multilin PR','IO Multilin CA') 
and nvl(VENDOR_NAME,'XXX') in ('GE POWER MANAGEMENT S A','GE POWER MANAGEMENT SL') THEN 'M7'
WHEN UPPER(IMPORTING_TEXTFILE) LIKE UPPER('%MARKHAM%') 
THEN NVL(trim(SITE),(case when WAREHOUSE_NAME ='IO Multilin US' then 'C58'
WHEN WAREHOUSE_NAME ='IO Multilin PR' THEN 'C57'
when WAREHOUSE_NAME ='IO Multilin CA' then 'C55' 
else PLANTCODE end))
ELSE PLANTCODE END PlantCode,
trim(IOBF.STATECODE) statecode,
  IBHD.TIER1 PandL_Tier1,
  IBHD.TIER2 PandL_Tier2,
  CASE
   WHEN length(regexp_replace(IMPORTING_TEXTFILE,'[0-9]','')) = 0 OR IMPORTING_TEXTFILE IS NULL
   THEN NVL(IDSD.DATA_SOURCE_NAME,IOBF.SOURCE)
   ELSE importing_textfile END as Source,
   IOBF.NETQUANTITY as NetQuantity,
trim(IOBF.CATALOGNUMBER) as CatalogNumber,
 IOBF.PRODUCTCLASS as ProductClass,
  regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(IOBF.CUSTOMERPONUMBER, decode(unhex(hex(3)), 'US-ASCII'),''),decode(unhex(hex(14)), 'US-ASCII'),''),decode(unhex(hex(15)), 'US-ASCII'),''),decode(unhex(hex(16)), 'US-ASCII'),''),decode(unhex(hex(17)), 'US-ASCII'),''),decode(unhex(hex(13)), 'US-ASCII'),''),decode(unhex(hex(10)), 'US-ASCII'),''),decode(unhex(hex(11)), 'US-ASCII'),'') AS  CustomerPONumber,
  substr(IOBF.customerrequestdate,1,10) as  CustomerRequestDate,
 IOBF.INVOICE_CURRENCY_CODE AS foreigncurrencycode,
  IOBF.LINENBR as LineNbr,
IOBF.INVOICENUMBER as InvoiceNumber,
  substr(IOBF.INVOICEDATE,1,10) as InvoiceDate,  
  substr(IOBF.SHIPDATE,1,10) as ShipDate,
  IOBF.ORDER_TYPE_PULSE as OrderType,
   trim(IOBF.SALES_ENGINEER) as SalesEngineer,
  trim(IOBF.SALESENGINEERCODE) as SalesEngineerCode,
  substr(IOBF.CUSTOMERORDERDATE,1,10) as CustomerOrderDate,
  trim(IOBF.SHIPTO_COUNTRY) as shipto_Country,  
    trim(IOBF.SHIP_STATE_CODE) as ship_to_state,
  trim(IOBF.SOLDTO_COUNTRY) as Soldto_Country,
  trim(IOBF.SOLDTO_STATE) as Soldto_State,
  trim(IOBF.PROJECT_NUMBER) as Project_Number,  
   trim(IOBF.REVISEDCHANNEL) Revised_Channel,
 trim(IOBF.BILLTO_SITENUMBER) as billto_sitenumber,
 trim(IOBF.SOLDTO_SITENUMBER) as Soldto_sitenumber,
   trim(IOBF.SHIPTO_SITENUMBER) as Shipto_sitenumber,
nvl(IOBF.MATERIAL,'') as material,
nvl(IOBF.LABOR,'') as labor,
nvl(IOBF.OVERHEAD,'') as overhead,
IOBF.CUSTOMERGROUP as CustomerClassification,
IOBF.PRODUCTCLASSGROUP as PRODUCT_GROUP,
RTRIM(LTRIM(UPPER(CASE WHEN IOBF.CREATED_BY IN ('iqp_sales_agg_erp') AND IOBF.WAREHOUSE_NAME IN ('IO Multilin CA', 'IO Multilin PR', 'IO Multilin US')
THEN NVL(IOBF.SOLDTO_STATE,NVL(IOBF.SHIP_STATE_CODE,IOBF.BILL_TO_STATE) )   
else NVL(IOBF.SHIP_STATE_CODE,IOBF.BILL_TO_STATE) END))) COUNTRY_STATE,
trim(IOBF.plantdescription) as PlantDescription,
IOBF.PRODUCT_SEGMENT as PRODUCT_SEGMENT,
IOBF.PRODUCT_SUB_LINE as PRODUCT_SUB_LINE,
concat(IOBF.PRODUCT_PLATFORM ,' ',IOBF.PRODUCTCLASSGROUP) as MAN_ADJ_PDK_CLASS, -- new column added
CASE WHEN IBHD.TIER2='GRID AUTOMATION'  AND IBHD.TIER3='PROTECTION & CONTROL' THEN ORDER_TYPE_PULSE ELSE '' END AS SUB_BUSINESS,
trim(IOBF.REGION) as DE_REGION,
trim(IOBF.SUB_REGION) as DE_SUB_REGION,
regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(IOBF.BILL_TO_COUNTRY,  decode(unhex(hex(3)), 'US-ASCII'),''),decode(unhex(hex(14)), 'US-ASCII'),''),decode(unhex(hex(15)), 'US-ASCII'),''),decode(unhex(hex(16)), 'US-ASCII'),''),decode(unhex(hex(17)), 'US-ASCII'),''),decode(unhex(hex(13)), 'US-ASCII'),''),decode(unhex(hex(10)), 'US-ASCII'),''),decode(unhex(hex(11)), 'US-ASCII'),'') as BILL_TO_COUNTRY, -- added to match FD
regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(IOBF.BILL_TO_STATE,  decode(unhex(hex(3)), 'US-ASCII'),''),decode(unhex(hex(14)), 'US-ASCII'),''),decode(unhex(hex(15)), 'US-ASCII'),''),decode(unhex(hex(16)), 'US-ASCII'),''),decode(unhex(hex(17)), 'US-ASCII'),''),decode(unhex(hex(13)), 'US-ASCII'),''),decode(unhex(hex(10)), 'US-ASCII'),''),decode(unhex(hex(11)), 'US-ASCII'),'') as BILL_TO_STATE, -- added to match FD
regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(IOBF.PRODUCT_PLATFORM,  decode(unhex(hex(3)), 'US-ASCII'),''),decode(unhex(hex(14)), 'US-ASCII'),''),decode(unhex(hex(15)), 'US-ASCII'),''),decode(unhex(hex(16)), 'US-ASCII'),''),decode(unhex(hex(17)), 'US-ASCII'),''),decode(unhex(hex(13)), 'US-ASCII'),''),decode(unhex(hex(10)), 'US-ASCII'),''),decode(unhex(hex(11)), 'US-ASCII'),'') as PRODUCT_PLATFORM, -- added to match FD
regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(IOBF.END_USER_CITY,  decode(unhex(hex(3)), 'US-ASCII'),''),decode(unhex(hex(14)), 'US-ASCII'),''),decode(unhex(hex(15)), 'US-ASCII'),''),decode(unhex(hex(16)), 'US-ASCII'),''),decode(unhex(hex(17)), 'US-ASCII'),''),decode(unhex(hex(13)), 'US-ASCII'),''),decode(unhex(hex(10)), 'US-ASCII'),''),decode(unhex(hex(11)), 'US-ASCII'),'') as END_USER_CITY, -- added to match FD
regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(IOBF.END_USER_COUNTRY,  decode(unhex(hex(3)), 'US-ASCII'),''),decode(unhex(hex(14)), 'US-ASCII'),''),decode(unhex(hex(15)), 'US-ASCII'),''),decode(unhex(hex(16)), 'US-ASCII'),''),decode(unhex(hex(17)), 'US-ASCII'),''),decode(unhex(hex(13)), 'US-ASCII'),''),decode(unhex(hex(10)), 'US-ASCII'),''),decode(unhex(hex(11)), 'US-ASCII'),'') as END_USER_COUNTRY, -- added to match FD
regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(IOBF.END_USER_NAME,  decode(unhex(hex(3)), 'US-ASCII'),''),decode(unhex(hex(14)), 'US-ASCII'),''),decode(unhex(hex(15)), 'US-ASCII'),''),decode(unhex(hex(16)), 'US-ASCII'),''),decode(unhex(hex(17)), 'US-ASCII'),''),decode(unhex(hex(13)), 'US-ASCII'),''),decode(unhex(hex(10)), 'US-ASCII'),''),decode(unhex(hex(11)), 'US-ASCII'),'') as END_USER_NAME, -- added to match FD
regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(IOBF.END_USER_STATE,  decode(unhex(hex(3)), 'US-ASCII'),''),decode(unhex(hex(14)), 'US-ASCII'),''),decode(unhex(hex(15)), 'US-ASCII'),''),decode(unhex(hex(16)), 'US-ASCII'),''),decode(unhex(hex(17)), 'US-ASCII'),''),decode(unhex(hex(13)), 'US-ASCII'),''),decode(unhex(hex(10)), 'US-ASCII'),''),decode(unhex(hex(11)), 'US-ASCII'),'') as END_USER_STATE, -- added to match FD
regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(IOBF.PRODUCTLINE,  decode(unhex(hex(3)), 'US-ASCII'),''),decode(unhex(hex(14)), 'US-ASCII'),''),decode(unhex(hex(15)), 'US-ASCII'),''),decode(unhex(hex(16)), 'US-ASCII'),''),decode(unhex(hex(17)), 'US-ASCII'),''),decode(unhex(hex(13)), 'US-ASCII'),''),decode(unhex(hex(10)), 'US-ASCII'),''),decode(unhex(hex(11)), 'US-ASCII'),'') as PRODUCTLINE, -- added to match FD
trim(IOBF.BUSINESS_SEGMENT) as BUSINESS_SEGMENT,
nvl(IOBF.EARLIEST_ACCEPTABLE_DATE_SPAIN,'') as EARLIEST_ACCEPTABLE_DATE_SPAIN, --added for spain
IOBF.BAAN_PO,--added for spain
IOBF.ORACLE_PO_NUMBER,--added for spain
IF(IDSD.data_source_key = '10',lkp_order_number.ORDER_NUMBER,'') as ORACLE_ORDER_NUMBER,
IF(IDSD.data_source_key = '1', lkp_baan_order_number.ORDER_NUMBER,'' ) as BAAN_ORDER_NUMBER,
IOBF.SPA_PDQ_NUM,
nvl(IOBF.MATERIAL_OVERHEAD,'') as Material_Overhead,--added per Ayan's request
nvl(IOBF.OSP_COST ,'') as OSP_Cost,--added per Ayan's request
nvl(IDSD.DATA_SOURCE_NAME,'') as SELLING_UNIT
FROM g00103.IQP_SALES_AGG IOBF
LEFT JOIN g00103.IQP_DATA_SOURCE_DIM IDSD
ON IOBF.SOURCE=IDSD.DATA_SOURCE_KEY
LEFT OUTER JOIN g00103.IQP_BUSINESS_HIER_DIM IBHD
ON IOBF.plantcode_id=IBHD.plantcode_id
LEFT OUTER JOIN g00103.IQP_DATE_DIM IDD
ON to_date(IOBF.PROCESSDATE)=to_date(IDD.DATE_KEY)
LEFT JOIN 
(SELECT 
max(REQUISITION) as ORDER_NUMBER,
NVL(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(IBA.CUSTOMER_PO_NBR, decode(unhex(hex(3)), 'US-ASCII'),''),decode(unhex(hex(14)), 'US-ASCII'),''),decode(unhex(hex(15)), 'US-ASCII'),''),decode(unhex(hex(16)), 'US-ASCII'),''),decode(unhex(hex(17)), 'US-ASCII'),''),decode(unhex(hex(13)), 'US-ASCII'),''),decode(unhex(hex(10)), 'US-ASCII'),''),decode(unhex(hex(11)), 'US-ASCII'),''),'00') as CUSTOMER_PO_NBR
from g00103.IQP_BACKLOG_AGG IBA
WHERE IBA.DATA_SOURCE = '1' 
group by NVL(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(IBA.CUSTOMER_PO_NBR, decode(unhex(hex(3)), 'US-ASCII'),''),decode(unhex(hex(14)), 'US-ASCII'),''),decode(unhex(hex(15)), 'US-ASCII'),''),decode(unhex(hex(16)), 'US-ASCII'),''),decode(unhex(hex(17)), 'US-ASCII'),''),decode(unhex(hex(13)), 'US-ASCII'),''),decode(unhex(hex(10)), 'US-ASCII'),''),decode(unhex(hex(11)), 'US-ASCII'),''),'00')
) lkp_order_number on(lkp_order_number.CUSTOMER_PO_NBR=IOBF.BAAN_PO)
LEFT JOIN 
(SELECT 
max(REQUISITION) as ORDER_NUMBER,
NVL(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(IBA.CUSTOMER_PO_NBR, decode(unhex(hex(3)), 'US-ASCII'),''),decode(unhex(hex(14)), 'US-ASCII'),''),decode(unhex(hex(15)), 'US-ASCII'),''),decode(unhex(hex(16)), 'US-ASCII'),''),decode(unhex(hex(17)), 'US-ASCII'),''),decode(unhex(hex(13)), 'US-ASCII'),''),decode(unhex(hex(10)), 'US-ASCII'),''),decode(unhex(hex(11)), 'US-ASCII'),''),'0')  as CUSTOMER_PO_NBR
from g00103.IQP_BACKLOG_AGG IBA
WHERE IBA.DATA_SOURCE = '10' 
group by 
NVL(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(IBA.CUSTOMER_PO_NBR, decode(unhex(hex(3)), 'US-ASCII'),''),decode(unhex(hex(14)), 'US-ASCII'),''),decode(unhex(hex(15)), 'US-ASCII'),''),decode(unhex(hex(16)), 'US-ASCII'),''),decode(unhex(hex(17)), 'US-ASCII'),''),decode(unhex(hex(13)), 'US-ASCII'),''),decode(unhex(hex(10)), 'US-ASCII'),''),decode(unhex(hex(11)), 'US-ASCII'),''),'0')
) lkp_baan_order_number on(lkp_baan_order_number.CUSTOMER_PO_NBR=IOBF.ORACLE_PO_NUMBER)
WHERE IBHD.TIER3 <> 'POWER QUALITY'
${ if(!filter1.isEmpty && !filter2.isEmpty)s"AND to_date(IOBF.PROCESSDATE) BETWEEN TO_DATE($filter1) and TO_DATE($filter2) " else ""}
AND UPPER(IOBF.REPORT_PROCESS1)=UPPER('Pulse Reporting')
AND IOBF.PANDL_TIER2 <> 'ACLARA METERS' AND   IOBF.PANDL_TIER3<> 'ACLARA METERS'"""

var sales_df=spark.sql(sql)

import spark.implicits._ 

val file = s"""/tmp/iqp/extract_files/$filename"""

var temp=""
for(x<-sales_df.columns)
temp+=x+"~~"

spark.sparkContext.parallelize(Array(temp)).repartition(1).saveAsTextFile(file+"/1")
val fs1 = FileSystem.get(spark.sparkContext.hadoopConfiguration);
val filepath1 = fs1.globStatus(new Path(file+"/1/part*"))(0).getPath().getName();
fs1.rename(new Path(file+"/1/"+ filepath1), new Path(file+"/1.txt"));

sales_df.na.fill("").rdd.map(x => x.mkString("~~")).repartition(1).saveAsTextFile(file+"/2")
val fs2 = FileSystem.get(spark.sparkContext.hadoopConfiguration);
val filepath2 = fs2.globStatus(new Path(file+"/2/part*"))(0).getPath().getName();
fs2.rename(new Path(file+"/2/"+ filepath2), new Path(file+"/2.txt"));

fs1.delete(new Path(file+"/1"),true)
fs1.delete(new Path(file+"/2"),true)

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