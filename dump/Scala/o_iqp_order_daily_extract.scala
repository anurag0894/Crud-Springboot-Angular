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

object o_iqp_order_daily_extract
{
  val tableName = "g00103.iqp_order_daily_extract"
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
 
val sql =  s"""select
NVL(IOBF.BILL_TO_CUSTOMER_NUMBER,IOBF.SHIP_TO_CUSTOMER_NUMBER) AS CUSTOMER_NUMBER,
NVL(IOBF.BILL_TO_CUSTOMER_NAME,IOBF.SHIP_TO_CUSTOMER_NAME) as CUSTOMER_NAME,
--IOBF.BILL_TO_CUSTOMER_NUMBER,
--IOBF.SHIP_TO_CUSTOMER_NAME,
IOBF.CUSTOMER_GROUP,
IBHD.TIER3,
IOBF.PRODUCT_PLATFORM,
case when case when IBHD.TIER2='GRID AUTOMATION' AND IBHD.TIER3 like 'PROTECTION%CONTROL'
THEN LTRIM(RTRIM(IOBF.PRODUCT_PLATFORM))
WHEN UPPER(IBHD.TIER2) = 'GRID AUTOMATION' AND UPPER(IBHD.TIER3) = 'M&D'
THEN LTRIM(RTRIM(IOBF.PRODUCT_LINE))
ELSE LTRIM(RTRIM(IBHD.PNL)) END is null or case when IBHD.TIER2='GRID AUTOMATION' AND IBHD.TIER3 like 'PROTECTION%CONTROL'
THEN LTRIM(RTRIM(IOBF.PRODUCT_PLATFORM))
WHEN UPPER(IBHD.TIER2) = 'GRID AUTOMATION' AND UPPER(IBHD.TIER3) = 'M&D'
THEN LTRIM(RTRIM(IOBF.PRODUCT_LINE))
ELSE LTRIM(RTRIM(IBHD.PNL)) END = '' then 'TBD' else case when IBHD.TIER2='GRID AUTOMATION' AND IBHD.TIER3 like 'PROTECTION%CONTROL'
THEN LTRIM(RTRIM(IOBF.PRODUCT_PLATFORM))
WHEN UPPER(IBHD.TIER2) = 'GRID AUTOMATION' AND UPPER(IBHD.TIER3) = 'M&D'
THEN LTRIM(RTRIM(IOBF.PRODUCT_LINE))
ELSE LTRIM(RTRIM(IBHD.PNL)) END end AS PNL,
IBHD.SUBPNL,
IOBF.BILL_TO_COUNTRY,
IOBF.REGION,
IOBF.SUBREGION,
ORDER_NUMBER,
(CASE WHEN UPPER(IOBF.IMPORTING_TEXTFILE) LIKE '%MARKHAM%' THEN nvl(IOBF.SITE,(case when SHIP_FROM_WAREHOUSE_CODE='C58' then 'C58'
when SHIP_FROM_WAREHOUSE_CODE='C57' then 'C57'
when SHIP_FROM_WAREHOUSE_CODE='C55' then 'C55'
when SHIP_FROM_WAREHOUSE_CODE='M7' then 'M7'
else management_entity end)) ELSE  IOBF.MANAGEMENT_ENTITY END) AS PLANTCODE,
--(CASE WHEN UPPER(IOBF.IMPORTING_TEXTFILE) LIKE '%MARKHAM%' THEN IOBF.SITE ELSE IOBF.management_entity END) AS PLANTCODE,
--(CASE WHEN IOBF.IMPORTING_TEXTFILE LIKE '%MARKHAM%' THEN ILCLS.SITE ELSE ILCLS.PLANTCODE END) AS PLANTCODE,
nvl(IGD.STATE_CODE,'UNASSIGNED') as BILLTO_STATECODE,
IBHD.TIER1,
IBHD.TIER2,
CASE
WHEN length(REGEXP_REPLACE(IMPORTING_TEXTFILE,'[0-9]','')) = 0 OR IMPORTING_TEXTFILE IS NULL
THEN NVL(IDSD.DATA_SOURCE_NAME,IOBF.DATA_SOURCE)
ELSE IMPORTING_TEXTFILE END AS SOURCE,
IOBF.CATLOG_NUMBER,
--ILCLS.PRODUCTCLASS,
IOBF.PRODUCT_CLASS_CODE,
REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(IOBF.CUSTOMER_PO_NBR, decode(unhex(hex(3)),'ASCII'),''),decode(unhex(hex(14)),'ASCII'),''),decode(unhex(hex(15)),'ASCII'),''),decode(unhex(hex(16)),'ASCII'),''),decode(unhex(hex(17)),'ASCII'),''),decode(unhex(hex(13)),'ASCII'),''),decode(unhex(hex(10)),'ASCII'),''),decode(unhex(hex(11)),'ASCII'),'') AS CUSTOMER_PO_NBR,
--LTRIM(RTRIM(IPBD.FAMILY_BUSRPT)) as FAMILY_BUSRPT,
CASE
WHEN upper(IOBF.IMPORTING_TEXTFILE) LIKE '%EMEA%' AND upper(IOBF.MANAGEMENT_ENTITY) LIKE '%SA' AND IOBF.PROCESS_DATE >= TO_DATE('2012-04-02') THEN 'Service'
WHEN upper(IOBF.IMPORTING_TEXTFILE) like '%MARKHAM%' AND upper(IBHD.PNL)='LENTRONICS' AND upper(IBHD.SubPNL)='LENTRONICS' AND upper(IPBD.Family_BusRpt)='UNCLASSIFIED' THEN 'Lentronics'
WHEN upper(IBHD.Tier3)='PROLEC' AND upper(IOBF.MAJOR_LINE_CODE)='TRI' THEN 'CT'
WHEN upper(IBHD.Tier3)='PROLEC' AND upper(IOBF.MAJOR_LINE_CODE)='IND' THEN 'IT'
WHEN upper(IBHD.Tier3)='PROLEC' AND upper(IOBF.MAJOR_LINE_CODE) IN ('POS','PED') THEN 'RT'
ELSE NVL(IPBD.FAMILY_BUSRPT,
CASE WHEN (upper(IOBF.IMPORTING_TEXTFILE) NOT LIKE '%ZULU%' AND upper(IOBF.IMPORTING_TEXTFILE) NOT LIKE '%ZEAL%') and (upper(IOBF.IMPORTING_TEXTFILE) like  '%T&D%' or upper(IOBF.IMPORTING_TEXTFILE) like '%MARKHAM%') THEN 'T&D Products'
else 'Unclassified' end)
END AS FAMILY_BUSRPT,
CURRENCY_CODE AS CURRENCY_CODE,
CASE
WHEN upper(IOBF.IMPORTING_TEXTFILE) like '%MARKHAM%' AND upper(IBHD.PNL)='LENTRONICS' AND upper(IBHD.SubPNL)='LENTRONICS' AND upper(IPBD.Family_BusRpt)='UNCLASSIFIED' THEN 'Lentronics'
WHEN upper(IBHD.Tier3)='PROLEC' AND upper(IOBF.MAJOR_LINE_CODE)='TRI' THEN 'TRI'
WHEN upper(IBHD.Tier3)='PROLEC' AND upper(IOBF.MAJOR_LINE_CODE)='IND' THEN 'IND'
WHEN upper(IBHD.Tier3)='PROLEC' AND upper(IOBF.MAJOR_LINE_CODE) ='POS' THEN 'POS'
WHEN upper(IBHD.Tier3)='PROLEC' AND upper(IOBF.MAJOR_LINE_CODE) ='PED' THEN 'PED'
ELSE NVL(IPBD.PRODUCTDESC_BUSRPT,
CASE WHEN (upper(IOBF.IMPORTING_TEXTFILE) NOT LIKE '%ZULU%' AND upper(IOBF.IMPORTING_TEXTFILE) NOT LIKE '%ZEAL%') and (upper(IOBF.IMPORTING_TEXTFILE) like  '%T&D%' or  upper(IOBF.IMPORTING_TEXTFILE) like '%MARKHAM%') THEN 'T&D Products'
ELSE 'Unclassified' END)
END AS PRODUCTDESCRIPTION_BUSRPT,
--IPBD.PRODUCTDESC_BUSRPT,
IOBF.ORDER_TYPE,
IOBF.SALES_ENGINEER_NAME,
IOBF.SALES_ENGINEER_CODE,
IOBF.SHIP_TO_COUNTRY,
IOBF.SHIP_TO_STATE,
IOBF.SOLD_TO_COUNTRY,
IOBF.SOLD_TO_STATE,
IOBF.PROCESS_DATE,
IOBF.CUSTOMER_REQUEST_DATE,
IOBF.ORDERED_DATE,
IOBF.NET_AMOUNT_USD NETAMOUNT,
IOBF.NET_AMOUNT_FUNC_CURRENCY ,
IOBF.NET_QUANTITY,
IDD.FISCAL_WEEK,
IDD.FISCAL_QUARTER,
IDD.FISCAL_YEAR,
IOBF.management_entity,
IOBF.BILL_TO_CLASSIFY,
CASE WHEN IOBF.DATA_SOURCE IN ('1','13') THEN SALES_CHANNEL
ELSE REVISED_CHANNEL END as REVISED_CHANNEL ,
IOBF.PROJECT_NUMBER,
CASE
WHEN IOBF.CUSTOMER_REQUEST_DATE between to_date('2014-01-01') AND to_date('2014-03-30') THEN '1Q14'
WHEN IOBF.CUSTOMER_REQUEST_DATE between TO_DATE('2014-03-31') AND TO_DATE('2014-06-29') THEN '2Q14'
WHEN IOBF.CUSTOMER_REQUEST_DATE between TO_DATE('2014-06-30') AND TO_DATE('2014-09-28') THEN '3Q14'
WHEN IOBF.CUSTOMER_REQUEST_DATE between TO_DATE('2014-09-29') AND TO_DATE('2014-12-31') THEN '4Q14'
WHEN IOBF.CUSTOMER_REQUEST_DATE between TO_DATE('2015-01-01') AND TO_DATE('2015-03-31') THEN '1Q15'
WHEN IOBF.CUSTOMER_REQUEST_DATE between TO_DATE('2015-04-01') AND TO_DATE('2015-06-30') THEN '2Q15'
WHEN IOBF.CUSTOMER_REQUEST_DATE between TO_DATE('2015-07-01') AND TO_DATE('2015-09-27') THEN '3Q15'
WHEN IOBF.CUSTOMER_REQUEST_DATE between TO_DATE('2015-09-28') AND TO_DATE('2016-01-02') THEN '4Q15'
WHEN IOBF.CUSTOMER_REQUEST_DATE >= TO_DATE('2016-01-01') THEN '2016+'
ELSE ''
END AS CO_FLAG,
IOBF.SHIP_TO_SITE_NUMBER,
IOBF.PRODUCTCLASSGROUP,
RTRIM(LTRIM(UPPER(case when
NVL(IOBF.ship_from_warehouse_code,'YY') in ('C55', 'C57', 'C58','M7')
and nvl(IOBF.SOLD_TO_COUNTRY, 'XX') <> nvl(IOBF.SHIP_TO_COUNTRY, 'YY')
and IOBF.SOLD_TO_COUNTRY is not null
then IOBF.SOLD_TO_COUNTRY
else IOBF.SHIP_TO_COUNTRY end))) country_name,
RTRIM(LTRIM(UPPER(case when
IOBF.ship_from_warehouse_code in ('C55', 'C57', 'C58','M7')
and nvl(IOBF.SOLD_TO_COUNTRY, 'XX') <> nvl(IOBF.SHIP_TO_COUNTRY, 'YY')
and IOBF.SOLD_TO_COUNTRY is not null
then IOBF.SOLD_TO_STATE
else IOBF.SHIP_TO_STATE end))) Country_State,
IOBF.PRODUCT_SEGMENT,
IOBF.PRODUCT_SUB_LINE,
IOBF.SHIP_FROM_WAREHOUSE_CODE,
CONCAT(IOBF.PRODUCT_PLATFORM,' ',IOBF.PRODUCTCLASSGROUP) as MAN_ADJ_PDK_CLASS,  -- new column added
CASE WHEN UPPER(IBHD.TIER2)='GRID AUTOMATION' AND UPPER(IBHD.TIER3)='PROTECTION & CONTROL' THEN ORDER_TYPE_PULSE ELSE '' END AS SUB_BUSINESS,
REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(IOBF.END_USER_CITY, decode(unhex(hex(3)),'ASCII'),''),decode(unhex(hex(14)),'ASCII'),''),decode(unhex(hex(15)),'ASCII'),''),decode(unhex(hex(16)),'ASCII'),''),decode(unhex(hex(17)),'ASCII'),''),decode(unhex(hex(13)),'ASCII'),''),decode(unhex(hex(10)),'ASCII'),''),decode(unhex(hex(11)),'ASCII'),'') AS END_USER_CITY,
REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(IOBF.END_USER_COUNTRY, decode(unhex(hex(3)),'ASCII'),''),decode(unhex(hex(14)),'ASCII'),''),decode(unhex(hex(15)),'ASCII'),''),decode(unhex(hex(16)),'ASCII'),''),decode(unhex(hex(17)),'ASCII'),''),decode(unhex(hex(13)),'ASCII'),''),decode(unhex(hex(10)),'ASCII'),''),decode(unhex(hex(11)),'ASCII'),'') AS END_USER_COUNTRY,
REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(IOBF.END_USER_NAME, decode(unhex(hex(3)),'ASCII'),''),decode(unhex(hex(14)),'ASCII'),''),decode(unhex(hex(15)),'ASCII'),''),decode(unhex(hex(16)),'ASCII'),''),decode(unhex(hex(17)),'ASCII'),''),decode(unhex(hex(13)),'ASCII'),''),decode(unhex(hex(10)),'ASCII'),''),decode(unhex(hex(11)),'ASCII'),'') AS END_USER_NAME,
REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(IOBF.END_USER_STATE, decode(unhex(hex(3)),'ASCII'),''),decode(unhex(hex(14)),'ASCII'),''),decode(unhex(hex(15)),'ASCII'),''),decode(unhex(hex(16)),'ASCII'),''),decode(unhex(hex(17)),'ASCII'),''),decode(unhex(hex(13)),'ASCII'),''),decode(unhex(hex(10)),'ASCII'),''),decode(unhex(hex(11)),'ASCII'),'') AS END_USER_STATE,
IOBF.BUSINESS_SEGMENT,
IOBF.BAAN_PO, --added for spain
IOBF.EARLIEST_ACCEPTABLE_DATE_SPAIN, -- added for spain
IOBF.ORACLE_PO_NUMBER, --added for spain
IOBF.DATA_SOURCE as DATA_SOURCE_1,
IOBF.LINE_NUMBER AS LINE_NUMBER
from
g00103.IQP_ORDER_BOOKING_FACT IOBF
LEFT OUTER JOIN g00103.IQP_DATA_SOURCE_DIM IDSD ON IOBF.DATA_SOURCE = cast(IDSD.DATA_SOURCE_KEY as String)
LEFT OUTER JOIN g00103.IQP_DATE_DIM IDD ON to_date(IOBF.PROCESS_DATE)=to_date(IDD.DATE_KEY)
LEFT OUTER JOIN (SELECT MAX(STATE_CODE) STATE_CODE,COUNTRY_NAME,STATE_NAME from g00103.IQP_GEOGRAPHY_DIM GROUP BY COUNTRY_NAME,STATE_NAME) IGD ON
upper(IOBF.SHIP_TO_COUNTRY)=upper(IGD.COUNTRY_NAME)
AND upper(IOBF.SHIP_TO_STATE)=upper(IGD.STATE_NAME)
LEFT OUTER JOIN g00103.IQP_BUSINESS_HIER_DIM IBHD ON IOBF.management_entity = IBHD.plantcode_id
LEFT OUTER JOIN g00103.IQP_PRODUCT_BUSINESS_REPORTS IPBD
ON( upper(rtrim(ltrim(IOBF.product_class_code)))=upper(rtrim(ltrim(IPBD.productclass_id_busrpt))) AND
upper(rtrim(ltrim(IOBF.CATLOG_NUMBER)))=upper(trim(ltrim(IPBD.CATALOGNUMBER_BUSRPT))) AND
UPPER(RTRIM(LTRIM(IOBF.MANAGEMENT_ENTITY)))=UPPER(RTRIM(LTRIM(IPBD.PLANTCODE_BUSRPT))) )
WHERE
IBHD.TIER3 != 'POWER QUALITY'
AND IBHD.TIER3 != 'ACLARA METERS'  AND    IBHD.TIER2 != 'ACLARA METERS'
AND REPORT_PROCESS1 = 'Pulse Reporting'
--AND IBHD.PNL !='PROLEC' AND IOBF.BILL_TO_CLASSIFY='External'
${ if(!filter1.isEmpty && !filter2.isEmpty)s"AND  IOBF.PROCESS_DATE  BETWEEN $filter1 and $filter2 " else ""}"""
val src = spark.sql(sql)
src.createOrReplaceTempView("source")

val lk1 = spark.sql("""select  distinct
ORDER_NUMBER as ORDER_NUMBER,
REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(IOBF.CUSTOMER_PO_NBR, decode(unhex(hex(3)),'ASCII'),''),decode(unhex(hex(14)),'ASCII'),''),decode(unhex(hex(15)),'ASCII'),''),decode(unhex(hex(16)),'ASCII'),''),decode(unhex(hex(17)),'ASCII'),''),decode(unhex(hex(13)),'ASCII'),''),decode(unhex(hex(10)),'ASCII'),''),decode(unhex(hex(11)),'ASCII'),'') AS CUSTOMER_PO_NBR
from
g00103.IQP_ORDER_BOOKING_FACT IOBF
where IOBF.data_source = '1'""")
lk1.createOrReplaceTempView("oracleorder")

val lk2 = spark.sql("""select  distinct
ORDER_NUMBER as ORDER_NUMBER,
REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(IOBF.CUSTOMER_PO_NBR, decode(unhex(hex(3)),'ASCII'),''),decode(unhex(hex(14)),'ASCII'),''),decode(unhex(hex(15)),'ASCII'),''),decode(unhex(hex(16)),'ASCII'),''),decode(unhex(hex(17)),'ASCII'),''),decode(unhex(hex(13)),'ASCII'),''),decode(unhex(hex(10)),'ASCII'),''),decode(unhex(hex(11)),'ASCII'),'') AS CUSTOMER_PO_NBR
from
g00103.IQP_ORDER_BOOKING_FACT IOBF
where IOBF.data_source = '10'""")
lk2.createOrReplaceTempView("baanorder")
 
val result1 = spark.sql("""select
substr(date_format(a.Process_Date,'MM/dd/yyyy'),1,10) as Process_Date,
a.CUSTOMER_NUMBER as CustomerNumber,
REGEXP_REPLACE(CUSTOMER_NAME,'"','') as CUSTOMER_NAME,
a.BILL_TO_CLASSIFY as CustomerGroup,
a.TIER3 as PandL_Tier3,
a.PNL as PandL,
a.SUBPNL as SubPandL,
a.BILL_TO_COUNTRY as country_name,
a.REGION as WorldPole,
a.SUBREGION as WorldSubPole,
a.FISCAL_WEEK as FiscalWeek,
SUBSTR(a.FISCAL_QUARTER,-1,1) as FiscalQuarter,
a.FISCAL_YEAR as CurrentYear,
a.ORDER_NUMBER as RequisitionNumber,
a.NETAMOUNT as NetAmount,
a.PLANTCODE as PlantCode,
a.BILLTO_STATECODE as Billto_statecode,
a.TIER1 as PandL_Tier1,
a.TIER2 as PandL_Tier2,
a.SOURCE as Source,
to_date(a.ORDERED_DATE) as OrderDate,
a.NET_QUANTITY as NetQuantity,
a.CATLOG_NUMBER as CatalogNumber,
a.PRODUCT_CLASS_CODE as ProductClass,
a.CUSTOMER_PO_NBR as CustomerPONumber,
substr(date_format(a.Customer_Request_Date,'MM/dd/yyyy'),1,10) as CustomerRequestDate,
a.FAMILY_BUSRPT as Family_BusRpt,
a.PRODUCTDESCRIPTION_BUSRPT as ProductDescription_BusRpt,
a.CURRENCY_CODE as foreigncurrencycode,
a.ORDER_TYPE as OrderType,
a.SALES_ENGINEER_NAME as SalesEngineer,
a.SALES_ENGINEER_CODE as SalesEngineerCode,
to_date(a.CUSTOMER_REQUEST_DATE) as LatestCRD,
a.BILL_TO_COUNTRY as Billto_Countryname,
a.SHIP_TO_COUNTRY as Shipto_Country,
a.SHIP_TO_STATE as Shipto_State,
a.SHIP_TO_SITE_NUMBER as Ship_to_Site_Number,
a.SOLD_TO_COUNTRY as Soldto_Country,
a.SOLD_TO_STATE as Soldto_State,
a.PROJECT_NUMBER as Project_Number,
a.REVISED_CHANNEL as REVISED_CHANNEL,
a.management_entity as Management_entity,
a.PRODUCT_PLATFORM as PRODUCT_PLATFORM,
a.CO_FLAG as CO_Flag,
a.Country_State as Country_State,
a.CUSTOMER_GROUP as CustomerClassification,
a.PRODUCTCLASSGROUP as PRODUCT_GROUP,
a.PRODUCT_SEGMENT as PRODUCT_SEGMENT,
a.PRODUCT_SUB_LINE as PRODUCT_SUB_LINE,
a.SHIP_FROM_WAREHOUSE_CODE as SHIP_FROM_WAREHOUSE_CODE,
a.MAN_ADJ_PDK_CLASS as MAN_ADJ_PDK_CLASS,
a.SUB_BUSINESS as SUB_BUSINESS,
a.REGION as DE_REGION,
a.SUBREGION as DE_SUB_REGION,
a.END_USER_CITY as END_USER_CITY,
a.END_USER_COUNTRY as END_USER_COUNTRY,
a.END_USER_NAME as END_USER_NAME,
a.END_USER_STATE as END_USER_STATE,
a.BUSINESS_SEGMENT as BUSINESS_SEGMENT,
to_date(a.EARLIEST_ACCEPTABLE_DATE_SPAIN) as EARLIEST_ACCEPTABLE_DATE_SPAIN,
a.BAAN_PO as BAAN_PO,
a.ORACLE_PO_NUMBER as ORACLE_PO_NUMBER,
case when a.DATA_SOURCE_1='1' then c.ORDER_NUMBER end as ORACLE_ORDER_NUMBER,
case when a.DATA_SOURCE_1='10' then b.ORDER_NUMBER end as BAAN_ORDER_NUMBER,
a.NET_AMOUNT_FUNC_CURRENCY as NET_LOCAL_AMOUNT,
a.LINE_NUMBER as LINE_NUMBER
from source a left join baanorder b on (b.CUSTOMER_PO_NBR = a.ORACLE_PO_NUMBER)
left join oracleorder c on (c.CUSTOMER_PO_NBR = a.BAAN_PO)
""")

val file = s"""/tmp/iqp/extract_files/$filename"""
val header = spark.sql("""select 'Process_Date','CustomerNumber','CustomerName','CustomerGroup','PandL_Tier3','PandL','SubPandL','country_name','WorldPole','WorldSubPole','FiscalWeek','FiscalQuarter','CurrentYear','RequisitionNumber','NetAmount','PlantCode','Billto_statecode','PandL_Tier1','PandL_Tier2','Source','OrderDate','NetQuantity','CatalogNumber','ProductClass','CustomerPONumber','CustomerRequestDate','Family_BusRpt','ProductDescription_BusRpt','foreigncurrencycode','OrderType','SalesEngineer','SalesEngineerCode','LatestCRD','Billto_Countryname','Shipto_Country','Shipto_State','Ship_to_Site_Number','Soldto_Country','Soldto_State','Project_Number','REVISED_CHANNEL','Management_entity','PRODUCT_PLATFORM','CO_Flag','Country_State','CustomerClassification','PRODUCT_GROUP','PRODUCT_SEGMENT','PRODUCT_SUB_LINE','SHIP_FROM_WAREHOUSE_CODE','MAN_ADJ_PDK_CLASS','SUB_BUSINESS','DE_REGION','DE_SUB_REGION','END_USER_CITY','END_USER_COUNTRY','END_USER_NAME','END_USER_STATE','BUSINESS_SEGMENT','EARLIEST_ACCEPTABLE_DATE_SPAIN','BAAN_PO','ORACLE_PO_NUMBER','ORACLE_ORDER_NUMBER','BAAN_ORDER_NUMBER','NET_LOCAL_AMOUNT','LINE_NUMBER'""")
val m = header.rdd.map(x => x.mkString("~~"))
m.repartition(1).saveAsTextFile(file+"/1")
val fs1 = FileSystem.get(spark.sparkContext.hadoopConfiguration);
val filepath1 = fs1.globStatus(new Path(file+"/1/part*"))(0).getPath().getName();
fs1.rename(new Path(file+"/1/"+ filepath1), new Path(file+"/1.txt"));

val p = result1.rdd.map(x => x.mkString("~~"))
p.repartition(1).saveAsTextFile(file+"/2")
val fs2 = FileSystem.get(spark.sparkContext.hadoopConfiguration);
val filepath2 = fs2.globStatus(new Path(file+"/2//part*"))(0).getPath().getName();
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