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

object o_co_daily_extract
{
  val tableName = "g00103.co_daily_extract"
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


val sql = s"""SELECT 
nvl(to_date(IOBF.PROCESS_DATE),'') `#ProcessDate`,
case 
when length(trim(nvl(cast(IOBF.ORDER_NUMBER as string),'')))<=6 and trim(nvl(NVL(IDSD.DATA_SOURCE_NAME,IOBF.DATA_SOURCE),''))='ALTAIS'
then cast(concat('0000',IOBF.ORDER_NUMBER) as string)
else cast(IOBF.ORDER_NUMBER as string)
end as RequisitionNumber,
nvl(IOBF.PRODUCT_LINE,'') PandL_ProductLine,
nvl(CASE   
WHEN upper(IDSD.data_SOURCE_name) LIKE '%EMEA%' AND upper(IOBF.MANAGEMENT_ENTITY) LIKE '%SA' AND to_date(IOBF.PROCESS_DATE) >= TO_DATE('2012-04-02') THEN 'Service' 
WHEN upper(IOBF.IMPORTING_TEXTFILE) like '%MARKHAM%' AND upper(IBHD.PNL)='LENTRONICS' AND upper(IBHD.SubPNL)='LENTRONICS' AND upper(IPBD.Family_BusRpt)='UNCLASSIFIED' THEN 'Lentronics' 
WHEN upper(IBHD.Tier3)='PROLEC' AND upper(IOBF.MAJOR_LINE_CODE)='TRI' THEN 'CT' 
WHEN upper(IBHD.Tier3)='PROLEC' AND upper(IOBF.MAJOR_LINE_CODE)='IND' THEN 'IT' 
WHEN upper(IBHD.Tier3)='PROLEC' AND upper(IOBF.MAJOR_LINE_CODE) IN ('POS','PED') THEN 'RT' 
ELSE NVL(IPBD.FAMILY_BUSRPT, 
            CASE WHEN (upper(IDSD.data_SOURCE_name) NOT LIKE '%ZULU%' AND upper(IDSD.data_SOURCE_name) NOT LIKE '%ZEAL%' )
and upper(IDSD.data_SOURCE_name) like  '%T&D%' and upper(IOBF.IMPORTING_TEXTFILE) like '%MARKHAM%'
THEN concat('T','&','D Products') 
            ELSE 'Unclassified' END )
END,'') AS Family_BusRpt,
nvl(CASE 
WHEN upper(IOBF.IMPORTING_TEXTFILE) like '%MARKHAM%' AND upper(IBHD.PNL)='LENTRONICS' AND upper(IBHD.SubPNL)='LENTRONICS' AND upper(IPBD.Family_BusRpt)='UNCLASSIFIED' THEN 'Lentronics' 
WHEN upper(IBHD.Tier3)='PROLEC' AND upper(IOBF.MAJOR_LINE_CODE)='TRI' THEN 'TRI' 
WHEN upper(IBHD.Tier3)='PROLEC' AND upper(IOBF.MAJOR_LINE_CODE)='IND' THEN 'IND' 
WHEN upper(IBHD.Tier3)='PROLEC' AND upper(IOBF.MAJOR_LINE_CODE) ='POS' THEN 'POS' 
WHEN upper(IBHD.Tier3)='PROLEC' AND upper(IOBF.MAJOR_LINE_CODE) ='PED' THEN 'PED' 
ELSE NVL(IPBD.PRODUCTDESC_BUSRPT, 
            CASE WHEN (upper(IDSD.data_SOURCE_name) NOT LIKE '%ZULU%' AND upper(IDSD.data_SOURCE_name) NOT LIKE '%ZEAL%' )
and upper(IDSD.data_SOURCE_name) like  '%T&D%' and upper(IOBF.IMPORTING_TEXTFILE) like '%MARKHAM%'
THEN concat('T','&','D Products') 
            ELSE 'Unclassified' END) 
END,'') AS ProductDescription_BusRpt,
nvl(regexp_replace(NVL(IOBF.BILL_TO_CUSTOMER_NAME,IOBF.SHIP_TO_CUSTOMER_NAME),'"',''),'') as CustomerName,
nvl(iobf.Bill_To_Classify,'') as CustomerGroup,
  nvl(IBHD.PNL,'') PandL_Pole,
  nvl(iobf.REGION,'') as WorldPole,
  nvl(iobf.subregion,'') as WorldSubPole,
  nvl(IGD.COUNTRY_NAME,'') Billto_Country,
  nvl(IGD.STATE_CODE,'') Billto_StateCode,
NVL((case when IBHD.TIER2='GRID AUTOMATION' AND IBHD.TIER3 like 'PROTECTION%CONTROL'
     THEN LTRIM(RTRIM(IOBF.PRODUCT_PLATFORM)) 
    WHEN UPPER(IBHD.TIER2) = 'GRID AUTOMATION' AND UPPER(IBHD.TIER3) = 'M&D'
                                                THEN LTRIM(RTRIM(IOBF.PRODUCT_LINE))
                                                
     ELSE LTRIM(RTRIM(IBHD.PNL)) END),'TBD') AS PandL ,  
	 nvl(IBHD.SUBPNL,'') SubPandL,
  NVL(IPBD.SUBFAMILY_BUSRPT,'TBD') AS SubFamily_BusRpt,
  nvl(iobf.PLANT_DESCRIPTION,'') PlantDescription,
  nvl(IOBF.Catlog_Number,'') as CatalogNumber,
    nvl(IBHD.TIER2,'') PandL_Tier2,
  nvl(IBHD.TIER3,'') PandL_Tier3,
    nvl(iobf.REGION,'') AS PandL_Pole_WorldPole,
  nvl(to_date(IOBF.CUSTOMER_REQUEST_DATE),'') LatestCRD,
 nvl(SUM(NET_AMOUNT_USD),'') NetAmount,
 nvl(SUM(IOBF.NET_QUANTITY),'') NetQuantity,
 nvl(SUM(IOBF.MATERIAL_COST),'') Material,
  nvl(SUM(IOBF.LABOR_COST),'') Labor,
  nvl(SUM(IOBF.OVERHEAD_COST),'') Overhead,
  nvl(SUM(IOBF.COMMISSIONS),'') Commissions,
  nvl(SUM(IOBF.FREIGHT),'') Freight,
nvl(case 
 when sales_flag_join.flag=1 then 'Y'
 else 'N' 
 end,'') as Shipped_Status,
   nvl(IOBF.ORDER_TYPE,'') OrderType,
  nvl(IOBF.SHIP_TO_COUNTRY,'') shipto_Country,
  nvl(IOBF.SHIP_TO_STATE,'') ship_to_state,
  nvl(IOBF.SOLD_TO_COUNTRY,'') as Soldto_Country,
  nvl(IOBF.SOLD_TO_STATE,'') as Soldto_State,
  nvl(IOBF.SHIP_TO_CUSTOMER_NUMBER,'') CustomerNumber,
  nvl(IOBF.CUSTOMER_GROUP,'') as CustomerClassification,
nvl(iobf.product_platform,'') as ProductPlatform,
nvl(iobf.productclassgroup,'') as ProductGroup,
nvl(NVL(IDSD.DATA_SOURCE_NAME,IOBF.DATA_SOURCE),'') AS Source,
 nvl(concat(IOBF.PRODUCT_PLATFORM ,' ',IOBF.PRODUCTCLASSGROUP),'') as MAN_ADJ_PDK_CLASS, 
 nvl(CASE WHEN UPPER(IBHD.TIER2)='GRID AUTOMATION' AND UPPER(IBHD.TIER3)='PROTECTION & CONTROL' THEN ORDER_TYPE_PULSE ELSE '' END,'') AS SUB_BUSINESS,
  nvl(iobf.REGION,'') as DE_REGION,
  nvl(iobf.subregion,'') as DE_SUB_REGION,
  nvl(to_date(IOBF.ORDERED_DATE),'') as ORDERED_DATE,
  nvl(IOBF.PRODUCT_LINE,'') as PRODUCT_LINE,
nvl(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(IOBF.END_USER_CITY, decode(unhex(hex(3)), 'US-ASCII'),''),decode(unhex(hex(14)), 'US-ASCII'),''),decode(unhex(hex(15)), 'US-ASCII'),''),decode(unhex(hex(16)), 'US-ASCII'),''),decode(unhex(hex(17)), 'US-ASCII'),''),decode(unhex(hex(13)), 'US-ASCII'),''),decode(unhex(hex(10)), 'US-ASCII'),''),decode(unhex(hex(11)), 'US-ASCII'),''),'') as END_USER_CITY,
nvl(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(IOBF.END_USER_COUNTRY, decode(unhex(hex(3)), 'US-ASCII'),''),decode(unhex(hex(14)), 'US-ASCII'),''),decode(unhex(hex(15)), 'US-ASCII'),''),decode(unhex(hex(16)), 'US-ASCII'),''),decode(unhex(hex(17)), 'US-ASCII'),''),decode(unhex(hex(13)), 'US-ASCII'),''),decode(unhex(hex(10)), 'US-ASCII'),''),decode(unhex(hex(11)), 'US-ASCII'),''),'') as END_USER_COUNTRY,
nvl(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(IOBF.END_USER_NAME, decode(unhex(hex(3)), 'US-ASCII'),''),decode(unhex(hex(14)), 'US-ASCII'),''),decode(unhex(hex(15)), 'US-ASCII'),''),decode(unhex(hex(16)), 'US-ASCII'),''),decode(unhex(hex(17)), 'US-ASCII'),''),decode(unhex(hex(13)), 'US-ASCII'),''),decode(unhex(hex(10)), 'US-ASCII'),''),decode(unhex(hex(11)), 'US-ASCII'),''),'') as END_USER_NAME,
nvl(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(IOBF.END_USER_STATE, decode(unhex(hex(3)), 'US-ASCII'),''),decode(unhex(hex(14)), 'US-ASCII'),''),decode(unhex(hex(15)), 'US-ASCII'),''),decode(unhex(hex(16)), 'US-ASCII'),''),decode(unhex(hex(17)), 'US-ASCII'),''),decode(unhex(hex(13)), 'US-ASCII'),''),decode(unhex(hex(10)), 'US-ASCII'),''),decode(unhex(hex(11)), 'US-ASCII'),''),'') as END_USER_STATE,
nvl(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(IOBF.CUSTOMER_PO_NBR, decode(unhex(hex(3)), 'US-ASCII'),''),decode(unhex(hex(14)), 'US-ASCII'),''),decode(unhex(hex(15)), 'US-ASCII'),''),decode(unhex(hex(16)), 'US-ASCII'),''),decode(unhex(hex(17)), 'US-ASCII'),''),decode(unhex(hex(13)), 'US-ASCII'),''),decode(unhex(hex(10)), 'US-ASCII'),''),decode(unhex(hex(11)), 'US-ASCII'),''),'') as CUSTOMER_PO_NBR,
nvl((CASE WHEN UPPER(IOBF.IMPORTING_TEXTFILE) LIKE '%MARKHAM%' THEN nvl(IOBF.SITE,(case when SHIP_FROM_WAREHOUSE_CODE='C58' then 'C58'
when SHIP_FROM_WAREHOUSE_CODE='C57' then 'C57'
when SHIP_FROM_WAREHOUSE_CODE='C55' then 'C55'
when SHIP_FROM_WAREHOUSE_CODE='M7' then 'M7'
ELSE MANAGEMENT_ENTITY END)) ELSE  IOBF.MANAGEMENT_ENTITY END),'') AS PLANTCODE,
nvl(IOBF.MANAGEMENT_ENTITY,'') MANAGEMENT_ENTITY,
nvl(IOBF.BUSINESS_SEGMENT,'') BUSINESS_SEGMENT,
nvl(to_date(IOBF.EARLIEST_ACCEPTABLE_DATE_SPAIN),'') EARLIEST_ACCEPTABLE_DATE_SPAIN, -- added for spain
nvl(IOBF.BAAN_PO,'') BAAN_PO, -- added for spain
nvl(IOBF.ORACLE_PO_NUMBER,'') ORACLE_PO_NUMBER,
nvl(IF(IDSD.DATA_SOURCE_key = '10',lkp_order_number.ORDER_NUMBER,''),'') as ORACLE_ORDER_NUMBER,
nvl(IF(IDSD.DATA_SOURCE_key = '1', lkp_baan_order_number.ORDER_NUMBER,'' ),'') as BAAN_ORDER_NUMBER,
   nvl(SUM(NET_AMOUNT_FUNC_CURRENCY),'') NET_LOCAL_AMOUNT,
  nvl(CURRENCY_CODE,'') AS CURRENCY_CODE,
'' as Dummy3,
'' as Dummy4,
'' as Dummy5,
'' as Dummy6
FROM g00103.IQP_ORDER_BOOKING_FACT IOBF
LEFT JOIN g00103.IQP_DATA_SOURCE_DIM IDSD ON IOBF.data_SOURCE=IDSD.DATA_SOURCE_KEY
LEFT OUTER JOIN g00103.IQP_DATE_DIM IDD ON to_date(IOBF.PROCESS_DATE)=to_date(IDD.DATE_KEY )
LEFT OUTER JOIN (select max(country_name) country_name ,country_code,state_code from  g00103.IQP_GEOGRAPHY_DIM 
                 group by country_code,state_code) IGD ON IOBF.ship_to_country=IGD.country_code and  IOBF.ship_to_state=IGD.state_code
LEFT OUTER JOIN g00103.IQP_BUSINESS_HIER_DIM IBHD ON IOBF.management_entity=IBHD.plantcode_id 
LEFT OUTER JOIN g00103.IQP_PRODUCT_BUSINESS_REPORTS IPBD
  ON( upper(rtrim(ltrim(IOBF.product_class_code)))=upper(rtrim(ltrim(IPBD.productclass_id_busrpt))) AND
  UPPER(RTRIM(LTRIM(IOBF.CATLOG_NUMBER)))=UPPER(TRIM(LTRIM(IPBD.CATALOGNUMBER_BUSRPT))) AND
  upper(rtrim(ltrim(IOBF.management_entity)))=upper(rtrim(ltrim(IPBD.PLANTCODE_BUSRPT))) )
LEFT OUTER JOIN 
(
SELECT 
         1 as flag,
        REQUISITIONNUMBER as REQUISITIONNUMBER ,
        CATALOGNUMBER as CATALOGNUMBER,
        regexp_replace(CUSTOMERNUMBER,'_EXT','') as CUSTOMERNUMBER
           FROM g00103.IQP_SALES_AGG
      WHERE REPORT_PROCESS1 = 'Pulse Reporting'
	  AND PROCESSDATE >= $filter1 
             AND REQUISITIONNUMBER IS NOT NULL
      AND CATALOGNUMBER     IS NOT NULL
      AND CUSTOMERNUMBER    IS NOT NULL 
	  group by REQUISITIONNUMBER,CATALOGNUMBER,regexp_replace(CUSTOMERNUMBER,'_EXT','')) sales_flag_join 
	  on(sales_flag_join.REQUISITIONNUMBER=IOBF.ORDER_NUMBER and
	     sales_flag_join.CATALOGNUMBER=IOBF.CATLOG_NUMBER and
		 sales_flag_join.CUSTOMERNUMBER=NVL(IOBF.BILL_TO_CUSTOMER_NUMBER,IOBF.SHIP_TO_CUSTOMER_NUMBER))
LEFT JOIN 
(SELECT 
max(ORDER_NUMBER) as ORDER_NUMBER,
NVL(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(IBA.CUSTOMER_PO_NBR, decode(unhex(hex(3)), 'US-ASCII'),''),decode(unhex(hex(14)), 'US-ASCII'),''),decode(unhex(hex(15)), 'US-ASCII'),''),decode(unhex(hex(16)), 'US-ASCII'),''),decode(unhex(hex(17)), 'US-ASCII'),''),decode(unhex(hex(13)), 'US-ASCII'),''),decode(unhex(hex(10)), 'US-ASCII'),''),decode(unhex(hex(11)), 'US-ASCII'),''),'00') as CUSTOMER_PO_NBR
from g00103.IQP_ORDER_BOOKING_FACT IBA
WHERE data_source='1'  
group by NVL(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(IBA.CUSTOMER_PO_NBR, decode(unhex(hex(3)), 'US-ASCII'),''),decode(unhex(hex(14)), 'US-ASCII'),''),decode(unhex(hex(15)), 'US-ASCII'),''),decode(unhex(hex(16)), 'US-ASCII'),''),decode(unhex(hex(17)), 'US-ASCII'),''),decode(unhex(hex(13)), 'US-ASCII'),''),decode(unhex(hex(10)), 'US-ASCII'),''),decode(unhex(hex(11)), 'US-ASCII'),''),'00')
) lkp_order_number on(lkp_order_number.CUSTOMER_PO_NBR=IOBF.BAAN_PO)

LEFT JOIN 
(SELECT 
max(ORDER_NUMBER) as ORDER_NUMBER,
NVL(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(IBA.CUSTOMER_PO_NBR, decode(unhex(hex(3)), 'US-ASCII'),''),decode(unhex(hex(14)), 'US-ASCII'),''),decode(unhex(hex(15)), 'US-ASCII'),''),decode(unhex(hex(16)), 'US-ASCII'),''),decode(unhex(hex(17)), 'US-ASCII'),''),decode(unhex(hex(13)), 'US-ASCII'),''),decode(unhex(hex(10)), 'US-ASCII'),''),decode(unhex(hex(11)), 'US-ASCII'),''),'0')  as CUSTOMER_PO_NBR
from g00103.IQP_ORDER_BOOKING_FACT IBA
WHERE data_source='10' 
group by 
NVL(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(IBA.CUSTOMER_PO_NBR, decode(unhex(hex(3)), 'US-ASCII'),''),decode(unhex(hex(14)), 'US-ASCII'),''),decode(unhex(hex(15)), 'US-ASCII'),''),decode(unhex(hex(16)), 'US-ASCII'),''),decode(unhex(hex(17)), 'US-ASCII'),''),decode(unhex(hex(13)), 'US-ASCII'),''),decode(unhex(hex(10)), 'US-ASCII'),''),decode(unhex(hex(11)), 'US-ASCII'),''),'0')
) lkp_baan_order_number on(lkp_baan_order_number.CUSTOMER_PO_NBR=IOBF.ORACLE_PO_NUMBER)
WHERE IOBF.REPORT_PROCESS1 = 'Pulse Reporting'
${ if(!filter1.isEmpty && !filter2.isEmpty) s"AND to_date(IOBF.PROCESS_DATE) BETWEEN TO_DATE($filter1) and TO_DATE($filter2) " else ""}
${ if(!filter1.isEmpty && !filter2.isEmpty) s"AND to_date(IOBF.CUSTOMER_REQUEST_DATE) BETWEEN TO_DATE($filter1) and TO_DATE($filter2) " else ""}
AND IBHD.TIER3 != 'ACLARA METERS'  AND    IBHD.TIER2 != 'ACLARA METERS'  
    AND IBHD.TIER3 != 'POWER QUALITY' 
 GROUP BY 
case 
when length(trim(nvl(cast(IOBF.ORDER_NUMBER as string),'')))<=6 and trim(nvl(NVL(IDSD.DATA_SOURCE_NAME,IOBF.DATA_SOURCE),''))='ALTAIS'
then cast(concat('0000',IOBF.ORDER_NUMBER) as string)
else cast(IOBF.ORDER_NUMBER as string)
end  ,
  nvl(IOBF.Catlog_Number,'') ,
  nvl(IOBF.SHIP_TO_CUSTOMER_NUMBER,'') ,
nvl(regexp_replace(NVL(IOBF.BILL_TO_CUSTOMER_NAME,IOBF.SHIP_TO_CUSTOMER_NAME),'"',''),'') ,
  nvl(IOBF.CUSTOMER_GROUP,'') ,
  nvl(IOBF.SHIP_TO_STATE,'') ,
  nvl(IOBF.SHIP_TO_COUNTRY,'') ,
  nvl(IOBF.SOLD_TO_STATE,'') ,
  nvl(IOBF.SOLD_TO_COUNTRY,'') ,
  nvl(to_date(IOBF.CUSTOMER_REQUEST_DATE),''),
  nvl(to_date(IOBF.PROCESS_DATE),''),
  nvl(iobf.REGION,'') ,
  nvl(iobf.subregion,'') ,
  nvl(CURRENCY_CODE,'') ,
  nvl(IOBF.ORDER_TYPE,'') ,
  nvl(IOBF.PRODUCT_LINE,'') ,
nvl(CASE   
WHEN upper(IDSD.data_SOURCE_name) LIKE '%EMEA%' AND upper(IOBF.MANAGEMENT_ENTITY) LIKE '%SA' AND to_date(IOBF.PROCESS_DATE) >= TO_DATE('2012-04-02') THEN 'Service' 
WHEN upper(IOBF.IMPORTING_TEXTFILE) like '%MARKHAM%' AND upper(IBHD.PNL)='LENTRONICS' AND upper(IBHD.SubPNL)='LENTRONICS' AND upper(IPBD.Family_BusRpt)='UNCLASSIFIED' THEN 'Lentronics' 
WHEN upper(IBHD.Tier3)='PROLEC' AND upper(IOBF.MAJOR_LINE_CODE)='TRI' THEN 'CT' 
WHEN upper(IBHD.Tier3)='PROLEC' AND upper(IOBF.MAJOR_LINE_CODE)='IND' THEN 'IT' 
WHEN upper(IBHD.Tier3)='PROLEC' AND upper(IOBF.MAJOR_LINE_CODE) IN ('POS','PED') THEN 'RT' 
ELSE NVL(IPBD.FAMILY_BUSRPT, 
            CASE WHEN (upper(IDSD.data_SOURCE_name) NOT LIKE '%ZULU%' AND upper(IDSD.data_SOURCE_name) NOT LIKE '%ZEAL%' )
and upper(IDSD.data_SOURCE_name) like  '%T&D%' and upper(IOBF.IMPORTING_TEXTFILE) like '%MARKHAM%'
THEN concat('T','&','D Products') 
            ELSE 'Unclassified' END )
END,''),
 nvl(CASE 
WHEN upper(IOBF.IMPORTING_TEXTFILE) like '%MARKHAM%' AND upper(IBHD.PNL)='LENTRONICS' AND upper(IBHD.SubPNL)='LENTRONICS' AND upper(IPBD.Family_BusRpt)='UNCLASSIFIED' THEN 'Lentronics' 
WHEN upper(IBHD.Tier3)='PROLEC' AND upper(IOBF.MAJOR_LINE_CODE)='TRI' THEN 'TRI' 
WHEN upper(IBHD.Tier3)='PROLEC' AND upper(IOBF.MAJOR_LINE_CODE)='IND' THEN 'IND' 
WHEN upper(IBHD.Tier3)='PROLEC' AND upper(IOBF.MAJOR_LINE_CODE) ='POS' THEN 'POS' 
WHEN upper(IBHD.Tier3)='PROLEC' AND upper(IOBF.MAJOR_LINE_CODE) ='PED' THEN 'PED' 
ELSE NVL(IPBD.PRODUCTDESC_BUSRPT, 
            CASE WHEN (upper(IDSD.data_SOURCE_name) NOT LIKE '%ZULU%' AND upper(IDSD.data_SOURCE_name) NOT LIKE '%ZEAL%' )
and upper(IDSD.data_SOURCE_name) like  '%T&D%' and upper(IOBF.IMPORTING_TEXTFILE) like '%MARKHAM%'
THEN concat('T','&','D Products') 
            ELSE 'Unclassified' END) 
END,'') ,

  nvl(IBHD.PNL,'') ,
  nvl(IGD.COUNTRY_NAME,'') ,
  nvl(IGD.STATE_CODE,'') ,
NVL((case when IBHD.TIER2='GRID AUTOMATION' AND IBHD.TIER3 like 'PROTECTION%CONTROL'
     THEN LTRIM(RTRIM(IOBF.PRODUCT_PLATFORM)) 
    WHEN UPPER(IBHD.TIER2) = 'GRID AUTOMATION' AND UPPER(IBHD.TIER3) = 'M&D'
                                                THEN LTRIM(RTRIM(IOBF.PRODUCT_LINE))
                                                
     ELSE LTRIM(RTRIM(IBHD.PNL)) END),'TBD')  ,  
  nvl(IBHD.SUBPNL,'') ,
  NVL(IPBD.SUBFAMILY_BUSRPT,'TBD') ,
  nvl(iobf.PLANT_DESCRIPTION,'') ,
  nvl(IBHD.TIER2,'') ,
  nvl(IBHD.TIER3,'') ,
  nvl(iobf.REGION,'') ,
nvl(iobf.Bill_To_Classify,'') ,
nvl(iobf.product_platform,'') ,
nvl(iobf.productclassgroup,'') ,
nvl(NVL(IDSD.DATA_SOURCE_NAME,IOBF.DATA_SOURCE),'') ,
nvl(NVL(IOBF.BILL_TO_CUSTOMER_NUMBER,IOBF.SHIP_TO_CUSTOMER_NUMBER),''),
nvl(case 
 when sales_flag_join.flag=1 then 'Y'
 else 'N' 
 end,'') ,
nvl(concat(IOBF.PRODUCT_PLATFORM ,' ',IOBF.PRODUCTCLASSGROUP),'')  , 
-- new column added
nvl(CASE WHEN UPPER(IBHD.TIER2)='GRID AUTOMATION' AND UPPER(IBHD.TIER3)='PROTECTION & CONTROL' THEN ORDER_TYPE_PULSE ELSE '' END,'') ,
nvl(to_date(IOBF.ORDERED_DATE),''),
nvl(IOBF.PRODUCT_LINE,''),
nvl(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(IOBF.END_USER_CITY, decode(unhex(hex(3)), 'US-ASCII'),''),decode(unhex(hex(14)), 'US-ASCII'),''),decode(unhex(hex(15)), 'US-ASCII'),''),decode(unhex(hex(16)), 'US-ASCII'),''),decode(unhex(hex(17)), 'US-ASCII'),''),decode(unhex(hex(13)), 'US-ASCII'),''),decode(unhex(hex(10)), 'US-ASCII'),''),decode(unhex(hex(11)), 'US-ASCII'),''),'') ,
nvl(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(IOBF.END_USER_COUNTRY, decode(unhex(hex(3)), 'US-ASCII'),''),decode(unhex(hex(14)), 'US-ASCII'),''),decode(unhex(hex(15)), 'US-ASCII'),''),decode(unhex(hex(16)), 'US-ASCII'),''),decode(unhex(hex(17)), 'US-ASCII'),''),decode(unhex(hex(13)), 'US-ASCII'),''),decode(unhex(hex(10)), 'US-ASCII'),''),decode(unhex(hex(11)), 'US-ASCII'),''),'') ,
nvl(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(IOBF.END_USER_NAME, decode(unhex(hex(3)), 'US-ASCII'),''),decode(unhex(hex(14)), 'US-ASCII'),''),decode(unhex(hex(15)), 'US-ASCII'),''),decode(unhex(hex(16)), 'US-ASCII'),''),decode(unhex(hex(17)), 'US-ASCII'),''),decode(unhex(hex(13)), 'US-ASCII'),''),decode(unhex(hex(10)), 'US-ASCII'),''),decode(unhex(hex(11)), 'US-ASCII'),''),'') ,
nvl(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(IOBF.END_USER_STATE, decode(unhex(hex(3)), 'US-ASCII'),''),decode(unhex(hex(14)), 'US-ASCII'),''),decode(unhex(hex(15)), 'US-ASCII'),''),decode(unhex(hex(16)), 'US-ASCII'),''),decode(unhex(hex(17)), 'US-ASCII'),''),decode(unhex(hex(13)), 'US-ASCII'),''),decode(unhex(hex(10)), 'US-ASCII'),''),decode(unhex(hex(11)), 'US-ASCII'),''),'') ,
nvl(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(IOBF.CUSTOMER_PO_NBR, decode(unhex(hex(3)), 'US-ASCII'),''),decode(unhex(hex(14)), 'US-ASCII'),''),decode(unhex(hex(15)), 'US-ASCII'),''),decode(unhex(hex(16)), 'US-ASCII'),''),decode(unhex(hex(17)), 'US-ASCII'),''),decode(unhex(hex(13)), 'US-ASCII'),''),decode(unhex(hex(10)), 'US-ASCII'),''),decode(unhex(hex(11)), 'US-ASCII'),''),'') ,

nvl((CASE WHEN UPPER(IOBF.IMPORTING_TEXTFILE) LIKE '%MARKHAM%' THEN nvl(IOBF.SITE,(case when SHIP_FROM_WAREHOUSE_CODE='C58' then 'C58'
when SHIP_FROM_WAREHOUSE_CODE='C57' then 'C57'
when SHIP_FROM_WAREHOUSE_CODE='C55' then 'C55'
when SHIP_FROM_WAREHOUSE_CODE='M7' then 'M7'
ELSE MANAGEMENT_ENTITY END)) ELSE  IOBF.MANAGEMENT_ENTITY END),'') ,
nvl(IOBF.MANAGEMENT_ENTITY,''),
nvl(IOBF.BUSINESS_SEGMENT,''),
nvl(IOBF.BAAN_PO,''), -- added for spain
nvl(IF(IDSD.DATA_SOURCE_key = '10',lkp_order_number.ORDER_NUMBER,''),'') ,
nvl(IF(IDSD.DATA_SOURCE_key = '1', lkp_baan_order_number.ORDER_NUMBER,'' ),'') ,
nvl(to_date(IOBF.EARLIEST_ACCEPTABLE_DATE_SPAIN),''), -- added for spain
nvl(IOBF.ORACLE_PO_NUMBER,''),
nvl(IOBF.DATA_SOURCE,'')"""

val co_daily_df=spark.sql(sql)

import spark.implicits._ 

val file = s"""/tmp/iqp/extract_files/$filename"""

var temp=""
for(x<-co_daily_df.columns)
temp+=x+"~~"

spark.sparkContext.parallelize(Array(temp)).repartition(1).saveAsTextFile(file+"/1")
val fs1 = FileSystem.get(spark.sparkContext.hadoopConfiguration);
val filepath1 = fs1.globStatus(new Path(file+"/1/part*"))(0).getPath().getName();
fs1.rename(new Path(file+"/1/"+ filepath1), new Path(file+"/1.txt"));

co_daily_df.na.fill("").rdd.map(x => x.mkString("~~")).repartition(1).saveAsTextFile(file+"/2")
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