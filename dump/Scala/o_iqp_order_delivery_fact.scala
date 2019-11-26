import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.DataFrame
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import java.io.File
import org.apache.spark.storage.StorageLevel
import java.text.SimpleDateFormat

object o_iqp_order_delivery_fact
{
  val tableName = "g00103.iqp_order_delivery_fact"
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

import spark.implicits._

val Master_view_DF=spark.sql("select * from g00103.ORD_DLVRY_FACT_VW")
Master_view_DF.createOrReplaceTempView("Master_view_VW")
spark.sql("CACHE TABLE Master_view_VW")

if (deBug) Utils.printLog(deBug, tableName + ": DF Master_view_DF:" + formatter.format(spark.sql("select 1 from Master_view_VW").count))


//changed group 
val Lkp_Source_name_DF=spark.sql("select NAME,ORDER_SOURCE_ID from  g00103.OE_ORDER_SOURCES")
Lkp_Source_name_DF.createOrReplaceTempView("Lkp_Source_name_VW")

if (deBug) Utils.printLog(deBug, tableName + ": DF Lkp_Source_name_DF:" + formatter.format(spark.sql("select 1 from Lkp_Source_name_VW").count))

//changed group
val Lkp_ORG_UNIT_NAME_DF=spark.sql("""select ORG_UNIT_NAME as ORG_UNIT_NAME ,ORGANIZATION_ID
from  g00103.iqp_ORGANIZATION_DIM  """)
Lkp_ORG_UNIT_NAME_DF.createOrReplaceTempView("Lkp_ORG_UNIT_NAME_VW")

if (deBug) Utils.printLog(deBug, tableName + ": DF Lkp_ORG_UNIT_NAME_DF:" + formatter.format(spark.sql("select 1 from Lkp_ORG_UNIT_NAME_VW").count))

//no need for group by(done)
val Lkp_DESCRIPTION_DF=spark.sql("""SELECT FFV.FLEX_VALUE AS FLEX_VALUE, FFVT.DESCRIPTION AS DESCRIPTION
 FROM  g00103.FND_FLEX_VALUE_SETS FFVS,  g00103.FND_FLEX_VALUES FFV,  g00103.FND_FLEX_VALUES_TL FFVT
 WHERE FFVS.FLEX_VALUE_SET_ID=FFV.FLEX_VALUE_SET_ID AND FFV.FLEX_VALUE_ID=FFVT.FLEX_VALUE_ID
 AND FFV.ENABLED_FLAG='Y' AND FFVT.LANGUAGE='US'
 AND FFVS.FLEX_VALUE_SET_NAME='PS_OM_COMMERCIAL_LEADER'
 """)
 Lkp_DESCRIPTION_DF.createOrReplaceTempView("Lkp_DESCRIPTION_VW")
 
 if (deBug) Utils.printLog(deBug, tableName + ": DF Lkp_DESCRIPTION_DF:" + formatter.format(spark.sql("select 1 from Lkp_DESCRIPTION_VW").count))

val Lkp_Job_Date_DF=spark.sql("""SELECT             
   DESCRETE_JOB.SOURCE_LINE_ID          AS ORDER_LINE_ID,       
   max(ENTITY.WIP_ENTITY_NAME)               AS WIP_ENTITY_NAME,      
   max(DESCRETE_JOB.CREATION_DATE)             AS JOB_CREATION_DATE,        
   max(DESCRETE_JOB.SCHEDULED_COMPLETION_DATE) AS JOB_SCHEDULED_COMPLETION_DATE,       
   max(DESCRETE_JOB.DATE_COMPLETED)            AS JOB_COMPLETED_DATE  
   FROM  g00103.MTL_RESERVATIONS        RESERVE inner join       
   g00103.WIP_ENTITIES             ENTITY on(RESERVE.SUPPLY_SOURCE_HEADER_ID = ENTITY.WIP_ENTITY_ID) inner join         
   g00103.WIP_DISCRETE_JOBS            DESCRETE_JOB on(ENTITY.WIP_ENTITY_ID = DESCRETE_JOB.WIP_ENTITY_ID)        
   group by  DESCRETE_JOB.SOURCE_LINE_ID  """)

Lkp_Job_Date_DF.createOrReplaceTempView("Lkp_Job_Date_VW")


 if (deBug) Utils.printLog(deBug, tableName + ": DF Lkp_Job_Date_DF:" + formatter.format(spark.sql("select 1 from Lkp_Job_Date_VW").count))


   val Lkp_Trx_all_DF=spark.sql("""select max(rct.trx_number) invoice_number
      ,max(rct.trx_date) invoice_date
      ,max(rcl.revenue_amount) EXTENDED_VALUE,
	  rcl.interface_line_attribute6 order_line_id
from  g00103.RA_CUSTOMER_TRX_ALL rct
      inner join  g00103.RA_CUST_TRX_TYPES_ALL rctt on (rct.CUST_TRX_TYPE_ID = rctt.CUST_TRX_TYPE_ID
                                                           and rct.org_id = rctt.org_id)
      inner join g00103.ra_customer_trx_lines_all rcl on (rct.CUSTOMER_TRX_ID = rcl.CUSTOMER_TRX_ID
                                                             and rct.org_id = rcl.org_id
                                                             and rcl.interface_line_context = 'ORDER ENTRY')
	where  rctt.TYPE = 'INV'
	group by rcl.interface_line_attribute6
	""")
Lkp_Trx_all_DF.createOrReplaceTempView("Lkp_Trx_all_VW")
spark.sql("CACHE TABLE Lkp_Trx_all_VW")

 if (deBug) Utils.printLog(deBug, tableName + ": DF Lkp_Trx_all_DF:" + formatter.format(spark.sql("select 1 from Lkp_Trx_all_VW").count))

val Lkp_Trx_Extended_DF=spark.sql("""select max(TRX_NUMBER) AS TRX_NUMBER, 
max(TRX_DATE) AS TRX_DATE, 
 SALES_ORDER_LINE AS SALES_ORDER_LINE,
 SALES_ORDER_NUMBER AS SALES_ORDER_NUMBER,
 SERVICE_NUMBER AS SERVICE_NUMBER,
 SHIPMENT_NUMBER AS SHIPMENT_NUMBER from  g00103.EXTENDED_VALUE_REF
 group by SALES_ORDER_LINE,SALES_ORDER_NUMBER,SERVICE_NUMBER,SHIPMENT_NUMBER""")
Lkp_Trx_Extended_DF.createOrReplaceTempView("Lkp_Trx_Extended_VW")


 if (deBug) Utils.printLog(deBug, tableName + ": DF Lkp_Trx_Extended_DF:" + formatter.format(spark.sql("select 1 from Lkp_Trx_Extended_VW").count))

val Lkp_Cst_DF=spark.sql("""SELECT  INVENTORY_ITEM_ID AS INVENTORY_ITEM_ID,
 ORGANIZATION_ID AS ORGANIZATION_ID,
 MATERIAL_COST AS MATERIAL_COST, 
 RESOURCE_COST AS RESOURCE_COST, 
 OUTSIDE_PROCESSING_COST AS OUTSIDE_PROCESSING_COST,
 OVERHEAD_COST AS OVERHEAD_COST FROM  g00103.cst_item_costs
 WHERE COST_TYPE_ID = 1000 
""")
Lkp_Cst_DF.createOrReplaceTempView("Lkp_Cst_VW")
spark.sql("CACHE TABLE Lkp_Cst_VW")

 if (deBug) Utils.printLog(deBug, tableName + ": DF Lkp_Cst_DF:" + formatter.format(spark.sql("select 1 from Lkp_Cst_VW").count))

val Lkp_User_DF=spark.sql("""SELECT B.USER_ID AS PERSON_ID,
 max(A.FULL_NAME) as FULL_NAME 
 FROM g00103.PER_ALL_PEOPLE_F A,
 g00103.FND_USER B WHERE B.EMPLOYEE_ID=A.PERSON_ID group by B.USER_ID """)
Lkp_User_DF.createOrReplaceTempView("Lkp_User_VW")

 if (deBug) Utils.printLog(deBug, tableName + ": DF Lkp_User_DF:" + formatter.format(spark.sql("select 1 from Lkp_User_VW").count))

val Lkp_Itemwarehouse_DF=spark.sql("""SELECT   INVENTORY_ITEM_ID AS INVENTORY_ITEM_ID,  
ORGANIZATION_ID AS ORGANIZATION_ID, 
 max(ATTRIBUTE10) AS ATTRIBUTE10 
 FROM  g00103.iqp_ITEMWAREHOUSE_DIM 
 where COST_TYPE_ID = 2 
group by  INVENTORY_ITEM_ID,ORGANIZATION_ID
  """)
Lkp_Itemwarehouse_DF.createOrReplaceTempView("Lkp_Itemwarehouse_VW")
spark.sql("CACHE TABLE Lkp_Itemwarehouse_VW")

 if (deBug) Utils.printLog(deBug, tableName + ": DF Lkp_Itemwarehouse_DF:" + formatter.format(spark.sql("select 1 from Lkp_Itemwarehouse_VW").count))

val Lkp_Conversion_DF=spark.sql("SELECT max(T1.CONVERSION_RATE) AS CONVERSION_RATE, T1.CONVERSION_DATE AS CONVERSION_DATE, T1.FROM_CURRENCY AS FROM_CURRENCY,TO_CURRENCY as TO_CURRENCY FROM g00103.GL_DAILY_RATES T1 WHERE  T1.CONVERSION_TYPE = '1000'  group by T1.CONVERSION_DATE,T1.FROM_CURRENCY,TO_CURRENCY")
Lkp_Conversion_DF.createOrReplaceTempView("Lkp_Conversion_VW")
spark.sql("CACHE TABLE Lkp_Conversion_VW")

 if (deBug) Utils.printLog(deBug, tableName + ": DF Lkp_Conversion_DF:" + formatter.format(spark.sql("select 1 from Lkp_Conversion_VW").count))


val Lkp_OE_TRANSACTION_TYPES_TL_all_DF=spark.sql("select NAME,TRANSACTION_TYPE_ID from g00103.OE_TRANSACTION_TYPES_TL")
Lkp_OE_TRANSACTION_TYPES_TL_all_DF.createOrReplaceTempView("Lkp_OE_TRANSACTION_TYPES_TL_all_VW")

 if (deBug) Utils.printLog(deBug, tableName + ": DF Lkp_OE_TRANSACTION_TYPES_TL_all_DF:" + formatter.format(spark.sql("select 1 from Lkp_OE_TRANSACTION_TYPES_TL_all_VW").count))


val Lkp_FND_LOOKUP_VALUES_all_DF=spark.sql("select LOOKUP_CODE,MEANING,LOOKUP_TYPE,ENABLED_FLAG,START_DATE_ACTIVE,END_DATE_ACTIVE from g00103.FND_LOOKUP_VALUES")
Lkp_FND_LOOKUP_VALUES_all_DF.createOrReplaceTempView("Lkp_FND_LOOKUP_VALUES_all_VW")
spark.sql("CACHE TABLE Lkp_FND_LOOKUP_VALUES_all_VW")

 if (deBug) Utils.printLog(deBug, tableName + ": DF Lkp_FND_LOOKUP_VALUES_all_DF:" + formatter.format(spark.sql("select 1 from Lkp_FND_LOOKUP_VALUES_all_VW").count))

val Lkp_FREIGHT_TERMS_DF=spark.sql("""SELECT T1.LOOKUP_CODE as LOOKUP_CODE, max(T1.MEANING) as FREIGHT_TERMS,LOOKUP_TYPE as LOOKUP_TYPE,ENABLED_FLAG as ENABLED_FLAG  
                      FROM  Lkp_FND_LOOKUP_VALUES_all_VW T1    group by T1.LOOKUP_CODE,T1.LOOKUP_TYPE,T1.ENABLED_FLAG """)
Lkp_FREIGHT_TERMS_DF.createOrReplaceTempView("Lkp_FREIGHT_TERMS_VW")
spark.sql("CACHE TABLE Lkp_FREIGHT_TERMS_VW")

 if (deBug) Utils.printLog(deBug, tableName + ": DF Lkp_FREIGHT_TERMS_DF:" + formatter.format(spark.sql("select 1 from Lkp_FREIGHT_TERMS_VW").count))

val Lkp_TRANSACTION_TYPE_ID_DF=spark.sql("""select distinct TRANSACTION_TYPE_ID AS TRANSACTION_TYPE_ID  from  Lkp_OE_TRANSACTION_TYPES_TL_all_VW where name in           
                       (select meaning  from  Lkp_FND_LOOKUP_VALUES_all_VW                 where lookup_type = 'GEPS_SOWO_PACKING_SLIP_EDI'   and enabled_flag = 'Y'             
					   and  to_date(current_timestamp) BETWEEN (case when to_date(START_DATE_ACTIVE) is null then date_add(current_timestamp, -1) 
					           else to_date(START_DATE_ACTIVE) end)  AND                       
							         (case when to_date(END_DATE_ACTIVE) is null then to_date(date_add(current_timestamp, +1)) 
									 else to_date(END_DATE_ACTIVE) end)) """)
Lkp_TRANSACTION_TYPE_ID_DF.createOrReplaceTempView("Lkp_TRANSACTION_TYPE_ID_VW")

 if (deBug) Utils.printLog(deBug, tableName + ": DF Lkp_TRANSACTION_TYPE_ID_DF:" + formatter.format(spark.sql("select 1 from Lkp_TRANSACTION_TYPE_ID_VW").count))

val Lkp_OE_ORDER_LINES_ALL_Sum_DF=spark.sql("""SELECT max(SUM(A.UNIT_SELLING_PRICE)) 
                                             OVER(PARTITION BY A.ORDER_HEADER_ID, A.LINE_NUMBER_STR, A.SHIPMENT_NUMBER) AS UNIT_SELLING_PRICE,
											 A.ORDER_HEADER_ID  AS HEADER_ID,   
											 A.LINE_NUMBER_STR  AS LINE_NUMBER,  
											 A.SHIPMENT_NUMBER  AS SHIPMENT_NUMBER
											 FROM  g00103.ORD_DLVRY_FACT_VW A 
											 WHERE A.ITEM_TYPE_CODE <> 'STANDARD'  AND A.SHIP_FROM_ORG_ID = 3719 AND A.UNIT_SELLING_PRICE != 0 
											 and OE_ACTUAL_SHIPMENT_DATE is not null 
											 group by ORDER_HEADER_ID,LINE_NUMBER_STR,SHIPMENT_NUMBER""")
Lkp_OE_ORDER_LINES_ALL_Sum_DF.createOrReplaceTempView("Lkp_OE_ORDER_LINES_ALL_Sum_VW")

 if (deBug) Utils.printLog(deBug, tableName + ": DF Lkp_OE_ORDER_LINES_ALL_Sum_DF:" + formatter.format(spark.sql("select 1 from Lkp_OE_ORDER_LINES_ALL_Sum_VW").count))

val Lkp_OE_ORDER_LINES_ALL_Config_DF=spark.sql("SELECT distinct TOP_MODEL_LINE_ID AS TOP_MODEL_LINE_ID FROM  Master_View_VW T1 WHERE ITEM_TYPE_CODE = 'CONFIG' ")
Lkp_OE_ORDER_LINES_ALL_Config_DF.createOrReplaceTempView("Lkp_OE_ORDER_LINES_ALL_Config_VW")

 if (deBug) Utils.printLog(deBug, tableName + ": DF Lkp_OE_ORDER_LINES_ALL_Config_DF:" + formatter.format(spark.sql("select 1 from Lkp_OE_ORDER_LINES_ALL_Config_VW").count))

val Lkp_OE_ORDER_LINES_ALL_Price_DF=spark.sql("SELECT SUM(UNIT_SELLING_PRICE) AS SALES_PRICE, T2.LINE_NUMBER_STR AS LINE_NUMBER, NVL(T2.SHIPMENT_NUMBER,0) AS SHIPMENT_NUMBER, NVL(T2.SERVICE_NUMBER,0) AS SERVICE_NUMBER, T2.ORDER_HEADER_ID AS HEADER_ID FROM  g00103.ORD_DLVRY_FACT_VW T2 where OE_ACTUAL_SHIPMENT_DATE is not null GROUP BY T2.LINE_NUMBER_STR, T2.SHIPMENT_NUMBER, NVL(T2.SERVICE_NUMBER,0), T2.ORDER_HEADER_ID")
Lkp_OE_ORDER_LINES_ALL_Price_DF.createOrReplaceTempView("Lkp_OE_ORDER_LINES_ALL_Price_VW")
spark.sql("CACHE TABLE Lkp_OE_ORDER_LINES_ALL_Price_VW")


 if (deBug) Utils.printLog(deBug, tableName + ": DF Lkp_OE_ORDER_LINES_ALL_Price_DF:" + formatter.format(spark.sql("select 1 from Lkp_OE_ORDER_LINES_ALL_Price_VW").count))

var Lkp_Mtl_Item_all_DF=spark.sql(""" select EFFECTIVITY_DATE,INVENTORY_ITEM_ID,ORGANIZATION_ID,REVISION from g00103.MTL_ITEM_REVISIONS_B where REVISION LIKE '%C%' or REVISION LIKE '%B%'""")
Lkp_Mtl_Item_all_DF.createOrReplaceTempView("Lkp_Mtl_Item_all_VW")

 if (deBug) Utils.printLog(deBug, tableName + ": DF Lkp_Mtl_Item_all_DF:" + formatter.format(spark.sql("select 1 from Lkp_Mtl_Item_all_VW").count))

val Lkp_Revisions_B_DF=spark.sql("""select T1.INVENTORY_ITEM_ID AS INVENTORY_ITEM_ID,                                      COUNT(*) AS TOTAL_B_REVISIONS,                      MIN(T1.EFFECTIVITY_DATE) AS REV_B_BOM_DATE, 					 T1.ORGANIZATION_ID AS ORGANIZATION_ID 					 FROM Lkp_Mtl_Item_all_VW T1 					 WHERE REVISION LIKE '%C%' GROUP BY T1.INVENTORY_ITEM_ID, T1.ORGANIZATION_ID""")
Lkp_Revisions_B_DF.createOrReplaceTempView("Lkp_Revisions_B_VW")

 if (deBug) Utils.printLog(deBug, tableName + ": DF Lkp_Revisions_B_DF:" + formatter.format(spark.sql("select 1 from Lkp_Revisions_B_VW").count))


//val Lkp_OE_LINES_CHANGED_REF_all_DF=spark.sql("select LINE_ID  from g00103.OE_LINES_CHANGED_REF")
//Lkp_OE_LINES_CHANGED_REF_all_DF.createOrReplaceTempView("Lkp_OE_LINES_CHANGED_REF_all_VW")
//spark.sql("CACHE TABLE Lkp_OE_LINES_CHANGED_REF_all_VW")
//val Lkp_OE_LINES_CHANGED_REF_Trx_DF=spark.sql("SELECT   max(T3.TRX_NUMBER) AS TRX_NUMBER,   CAST(T2.INTERFACE_LINE_ATTRIBUTE6 as INT) AS LINE_ID   FROM    Lkp_OE_LINES_CHANGED_REF_all_VW T1,    g00103.ra_customer_trx_lines_all T2,    g00103.RA_CUSTOMER_TRX_ALL T3    WHERE   T1.LINE_ID = CAST(T2.INTERFACE_LINE_ATTRIBUTE6 as INT) AND   T2.CUSTOMER_TRX_ID = T3.CUSTOMER_TRX_ID AND   T2.INTERFACE_LINE_CONTEXT = 'ORDER ENTRY'   group by CAST(T2.INTERFACE_LINE_ATTRIBUTE6 as INT)")
//Lkp_OE_LINES_CHANGED_REF_Trx_DF.createOrReplaceTempView("Lkp_OE_LINES_CHANGED_REF_Trx_VW")
val Lkp_OE_ORDER_HOLDS_ALL_all_DF=spark.sql("""select  oohl.header_id,cast(NULL as double) line_id, ohd.name as HOLD_NAME
      from (select header_id, max(order_hold_id) as order_hold_id
            from g00103.oe_order_holds_all
            where line_id is null
            and released_flag = 'N'
            group by header_id)  oohl_max
            inner join g00103.oe_order_holds_all  oohl on (oohl_max.order_hold_id = oohl.order_hold_id)
            inner join g00103.oe_hold_sources_All ohsa on (ohsa.hold_source_id = oohl.hold_source_id)
            inner join g00103.oe_hold_definitions ohd  on (ohsa.hold_id = ohd.hold_id)
      union
      select  oohl.header_id,  oohl.line_id as line_id ,  ohd.name as HOLD_NAME
      from (select header_id, line_id, max(order_hold_id) as order_hold_id
            from g00103.oe_order_holds_all
            where line_id is NOT null
            and released_flag = 'N'
            group by header_id, line_id)  oohl_max
            inner join g00103.oe_order_holds_all  oohl on (oohl_max.order_hold_id = oohl.order_hold_id)
            inner join g00103.oe_hold_sources_All ohsa on (ohsa.hold_source_id = oohl.hold_source_id)
            inner join g00103.oe_hold_definitions ohd  on (ohsa.hold_id = ohd.hold_id)""")
Lkp_OE_ORDER_HOLDS_ALL_all_DF.createOrReplaceTempView("Lkp_OE_ORDER_HOLDS_ALL_all_VW")
spark.sql("CACHE TABLE Lkp_OE_ORDER_HOLDS_ALL_all_VW")

 if (deBug) Utils.printLog(deBug, tableName + ": DF Lkp_OE_ORDER_HOLDS_ALL_all_DF:" + formatter.format(spark.sql("select 1 from Lkp_OE_ORDER_HOLDS_ALL_all_VW").count))

val Lkp_Customer_dim_all_DF=spark.sql("select ACCOUNT_NAME,PARTY_NAME,ACCOUNT_NUMBER,PARTY_NUMBER,CUST_ACCOUNT_ID,CUST_SITE_USES_SITE_USE_ID from g00103.iqp_CUSTOMER_DIM")
Lkp_Customer_dim_all_DF.createOrReplaceTempView("Lkp_Customer_dim_all_VW")

 if (deBug) Utils.printLog(deBug, tableName + ": DF Lkp_Customer_dim_all_DF:" + formatter.format(spark.sql("select 1 from Lkp_Customer_dim_all_VW").count))


val Lkp_Customer_dim_Party_DF=spark.sql("""SELECT max(NVL(T1.ACCOUNT_NAME,T1.PARTY_NAME)) AS PARTY_NAME, 
                                        max(NVL(T1.ACCOUNT_NUMBER,T1.PARTY_NUMBER)) AS ACCOUNT_NUMBER, 
										T1.CUST_ACCOUNT_ID AS CUST_ACCOUNT_ID FROM  Lkp_Customer_dim_all_VW T1, 
										(SELECT MAX(PARTY_NAME) PARTY_NAME,CUST_ACCOUNT_ID FROM  Lkp_Customer_dim_all_VW GROUP BY CUST_ACCOUNT_ID) T2 
										WHERE T1.CUST_ACCOUNT_ID = T2.CUST_ACCOUNT_ID AND 
										      T1.PARTY_NAME = T2.PARTY_NAME 
											  group by T1.CUST_ACCOUNT_ID
											  """)
Lkp_Customer_dim_Party_DF.createOrReplaceTempView("Lkp_Customer_dim_Party_VW")
spark.sql("CACHE TABLE Lkp_Customer_dim_Party_VW")

 if (deBug) Utils.printLog(deBug, tableName + ": DF Lkp_Customer_dim_Party_DF:" + formatter.format(spark.sql("select 1 from Lkp_Customer_dim_Party_VW").count))


val Lkp_Customer_dim_Cust_id_DF=spark.sql("SELECT max(NVL( T1.ACCOUNT_NAME,T1.PARTY_NAME)) as PARTY_NAME,   T1.CUST_SITE_USES_SITE_USE_ID as CUST_SITE_USES_SITE_USE_ID  FROM  Lkp_Customer_dim_all_VW T1  group by T1.CUST_SITE_USES_SITE_USE_ID  ")
Lkp_Customer_dim_Cust_id_DF.createOrReplaceTempView("Lkp_Customer_dim_Cust_id_VW")
spark.sql("CACHE TABLE Lkp_Customer_dim_Cust_id_VW")

 if (deBug) Utils.printLog(deBug, tableName + ": DF Lkp_Customer_dim_Cust_id_DF:" + formatter.format(spark.sql("select 1 from Lkp_Customer_dim_Cust_id_VW").count))

val Lkp_FND_Complexity_DF=spark.sql("""select
  fad.pk1_value order_header_id
, fad.entity_name
, fad.attached_document_id
, fd.category_id
, fdst.short_text as Req_Eng
, fdct.user_name description
from  (select pk1_value, max(attached_document_id) attached_document_id
       from g00103.fnd_attached_documents
       where entity_name =  'OE_ORDER_HEADERS'
       group by pk1_value) fad_max
      inner join g00103.fnd_attached_documents fad on (fad_max.attached_document_id = fad.attached_document_id)
      inner join g00103.fnd_documents fd on (fd.document_id = fad.document_id and fd.datatype_id = 1 )
      inner join g00103.fnd_documents_short_text fdst on (fdst.media_id = fd.media_id)
      inner join g00103.fnd_document_categories_tl fdct on (fdct.category_id = fd.category_id and fdct.language = 'US')""")
Lkp_FND_Complexity_DF.createOrReplaceTempView("Lkp_FND_Complexity_VW")

 if (deBug) Utils.printLog(deBug, tableName + ": DF Lkp_FND_Complexity_DF:" + formatter.format(spark.sql("select 1 from Lkp_FND_Complexity_VW").count))

val Lkp_FND_Req_DF=spark.sql("""select  
        fad.pk1_value order_line_id 
      , fad.entity_name
      , fad.attached_document_id
      , fd.category_id
      , fdst.short_text as Req_Eng
      , fdct.user_name description
      from  (select pk1_value, max(attached_document_id) attached_document_id
             from g00103.fnd_attached_documents 
             where entity_name =  'OE_ORDER_LINES'
             group by pk1_value) fad_max
            inner join g00103.fnd_attached_documents fad on (fad_max.attached_document_id = fad.attached_document_id)
            inner join g00103.fnd_documents fd on (fd.document_id = fad.document_id and fd.datatype_id = 1 )
            inner join g00103.fnd_documents_short_text fdst on (fdst.media_id = fd.media_id)
            inner join g00103.fnd_document_categories_tl fdct on (fdct.category_id = fd.category_id and fdct.language = 'US')""")
Lkp_FND_Req_DF.createOrReplaceTempView("Lkp_FND_Req_VW")

 if (deBug) Utils.printLog(deBug, tableName + ": DF Lkp_FND_Req_DF:" + formatter.format(spark.sql("select 1 from Lkp_FND_Req_VW").count))

val Lkp_Mtl_Item_Type_C_DF=spark.sql("""SELECT                      MIN(T1.EFFECTIVITY_DATE) AS REV_B_BOM_DATE, 					 T1.INVENTORY_ITEM_ID AS INVENTORY_ITEM_ID, 					 T1.ORGANIZATION_ID AS ORGANIZATION_ID, 					 COUNT(*) AS TOTAL_B_REVISIONS 					 FROM Lkp_Mtl_Item_all_VW T1 					 WHERE REVISION LIKE '%B%' GROUP BY T1.INVENTORY_ITEM_ID, T1.ORGANIZATION_ID""") 
Lkp_Mtl_Item_Type_C_DF.createOrReplaceTempView("Lkp_Mtl_Item_Type_C_VW")

 if (deBug) Utils.printLog(deBug, tableName + ": DF Lkp_Mtl_Item_Type_C_DF:" + formatter.format(spark.sql("select 1 from Lkp_Mtl_Item_Type_C_VW").count))

val finalResultToWrite = spark.sql("""select Master_view_VW.SOURCE_HEADER_TYPE_NAME,
Master_view_VW.DELIVERY_DETAIL_ID,
Master_view_VW.SOURCE_CODE,
Master_view_VW.ORDER_NUMBER,
Master_view_VW.LINE_NUMBER_STR,
Master_view_VW.CUST_PO_NUMBER,
Master_view_VW.HOLD_CODE,
Master_view_VW.CLASSIFICATION,
Master_view_VW.SRC_REQUESTED_QUANTITY,
Master_view_VW.SRC_REQUESTED_QUANTITY_UOM,
Master_view_VW.CANCELLED_QUANTITY,
Master_view_VW.REQUESTED_QUANTITY,
Master_view_VW.REQUESTED_QUANTITY_UOM,
Master_view_VW.SHIPPED_QUANTITY,
Master_view_VW.DELIVERED_QUANTITY,
Master_view_VW.QUALITY_CONTROL_QUANTITY,
Master_view_VW.CYCLE_COUNT_QUANTITY,
Master_view_VW.SUBINVENTORY,
Master_view_VW.REVISION,
Master_view_VW.LOT_NUMBER,
Master_view_VW.RELEASED_STATUS,
Master_view_VW.SERIAL_NUMBER,
Master_view_VW.LOAD_SEQ_NUMBER,
T5.FREIGHT_TERMS, 
Lkp_FREIGHT_TERMS_VW.FREIGHT_TERMS as FREIGHT_TERMS1,
Master_view_VW.SHIPMENT_PRIORITY,
T7.FREIGHT_TERMS as FREIGHT_TERM2,
Master_view_VW.NET_WEIGHT,
Master_view_VW.WEIGHT_UOM_CODE,
Master_view_VW.VOLUME,
Master_view_VW.VOLUME_UOM_CODE,
Master_view_VW.TRACKING_NUMBER,
Master_view_VW.CREATION_DATE,
Master_view_VW.MASTER_SERIAL_NUMBER,
Master_view_VW.GROSS_WEIGHT,
Master_view_VW.FILL_PERCENT,
Master_view_VW.CONTAINER_NAME,
Master_view_VW.PREFERRED_GRADE,
Master_view_VW.SUBLOT_NUMBER,
Master_view_VW.UNIT_PRICE,
Master_view_VW.CURRENCY_CODE,
Master_view_VW.UNIT_NUMBER,
Master_view_VW.INSPECTION_FLAG,
Master_view_VW.ORIGINAL_SUBINVENTORY,
Master_view_VW.PICKED_QUANTITY,
Master_view_VW.RECEIVED_QUANTITY,
Master_view_VW.SERVICE_LEVEL,
Master_view_VW.MODE_OF_TRANSPORT,
Master_view_VW.SHIPPING_CONTROL,
Master_view_VW.SOURCE_BLANKET_REFERENCE_NUM,
Master_view_VW.PO_SHIPMENT_LINE_NUMBER,
Master_view_VW.SCHEDULED_QUANTITY,
Master_view_VW.RETURNED_QUANTITY,
Master_view_VW.SOURCE_LINE_TYPE_CODE,
Master_view_VW.SUPPLIER_ITEM_NUMBER,
Master_view_VW.FILLED_VOLUME,
Master_view_VW.UNIT_WEIGHT,
Master_view_VW.UNIT_VOLUME,
Master_view_VW.WV_FROZEN_FLAG,
Master_view_VW.PO_REVISION_NUMBER,
Master_view_VW.RELEASE_REVISION_NUMBER,
Master_view_VW.NULL_FIELD,
Master_view_VW.NULL_FIELD,
Master_view_VW.NULL_FIELD,
Master_view_VW.FIRMWARE,
Lkp_OE_TRANSACTION_TYPES_TL_all_VW.NAME,
Lkp_Source_name_VW.NAME as name5,
Master_view_VW.ORDER_HEADER_STATUS,
Master_view_VW.TRANSACTION_CURRENCY,
Master_view_VW.ORDER_QUANTITY,
Master_view_VW.SPLIT_BY,
case 
when Master_view_VW.ITEM_TYPE_CODE='CONFIG' and UPPER(Lkp_ORG_UNIT_NAME_VW.ORG_UNIT_NAME)= 'IO ROCHESTER' 
then Lkp_OE_ORDER_LINES_ALL_Sum_VW.UNIT_SELLING_PRICE 
else Master_view_VW.UNIT_SELLING_PRICE 
End as UNIT_SELLING_PRICE, 
nvl(T53.CONVERSION_RATE,1),  
Master_view_VW.ORDER_BOOKING_DATE,
Master_view_VW.ORDER_LN_CREATION_DATE,
Master_view_VW.ORDER_REQUEST_DATE,
Master_view_VW.ORDER_PROMISE_DATE,
Master_view_VW.OE_SCHEDULE_SHIP_DATE,
Master_view_VW.OE_ACTUAL_SHIPMENT_DATE,
Master_view_VW.OE_EARLIEST_ACCEPTABLE_DATE,
Master_view_VW.NULL_FIELD as UNIT_SHIP_FLAG,
Master_view_VW.PAYMENT_TERM,
Master_view_VW.NULL_FIELD as HOLD_FLAG,
Lkp_OE_ORDER_HOLDS_ALL_all_VW.HOLD_NAME as HOLD_REASON,
Master_view_VW.SHIPPING_INSTRUCTIONS,
Master_view_VW.PACKING_INSTRUCTIONS,
Lkp_DESCRIPTION_VW.DESCRIPTION  as GE_CUSTOMER_CONTACT,
Master_view_VW.WAY_BILL_NUMBER,
Master_view_VW.PICK_SLIP_NUMBER,
Master_view_VW.END_USER,
Master_view_VW.LINE_STATUS_CODE,
Master_view_VW.ORDER_LINE_ID,
Master_view_VW.ORDER_HEADER_ID,
Master_view_VW.LINE_CATEGORY_CODE,
Master_view_VW.TRANSACTION_PHASE_CODE,
Master_view_VW.SHIPMENT_NUMBER,
 Lkp_Job_Date_VW.WIP_ENTITY_NAME ,
Master_view_VW.NULL_FIELD as JOB_STATUS,
Master_view_VW.ITEM_TYPE_CODE,
Master_view_VW.NULL_FIELD, 
 Master_view_VW.QTY_SHIPPED_LINE,
 case 
when Lkp_OE_ORDER_HOLDS_ALL_all_VW.HEADER_ID is null
then 'N' 
else 
'Y' 
end as ORDER_HOLD_FLAG,
case 
when Lkp_OE_ORDER_HOLDS_ALL_all_VW.LINE_ID is null
then 'N'
else 
'Y'
end as LINE_HOLD_FLAG,
Master_view_VW.ORDERED_DATE,
Master_view_VW.SOURCE_TYPE_CODE,
T72.FREIGHT_TERMS as FREIGHT_TERMS7 ,            
Master_view_VW.NULL_FIELD,						  
Master_view_VW.DELIVERY_ID,
Master_view_VW.BILL_OF_LADING_NUMBER,
Master_view_VW.FOB_POINT_CODE,
 Lkp_Customer_dim_Party_VW.ACCOUNT_NUMBER ,
 case 
when Master_view_VW.SOLD_TO_SITE_USE_ID is null
then Lkp_Customer_dim_Party_VW.PARTY_NAME
else 
Lkp_Customer_dim_Cust_id_VW.CUST_SITE_USES_SITE_USE_ID 
end as CUSTOM_FIELD2,
case 
when Master_view_VW.END_CUSTOMER_SITE_USE_ID is null
then
   case
     when Lkp_TRANSACTION_TYPE_ID_VW.TRANSACTION_TYPE_ID is null
     then Lkp_Customer_dim_Number_VW.PARTY_NAME
     else
     (SUBSTR(Master_view_VW.ATTRIBUTE17, 1, INSTR(Master_view_VW.ATTRIBUTE17, '|') - 1))
    end
else
T29.PARTY_NAME
end as CUSTOM_FIELD3,
Lkp_Trx_all_VW.EXTENDED_VALUE ,
Master_view_VW.SERVICE_NUMBER,
Master_view_VW.INVOICED_QUANTITY,
Master_view_VW.OL_ATTRIBUTE20,
 case 
  when Master_view_VW.ORDER_HEADER_STATUS IN('AWAITING_RECEIPT', 'CANCELLED', 'SHIPPED', 'CLOSED','AWAITING_RETURN', 'AWAITING_RETURN_DISPOSITION', 'INVOICED')
            and (Master_view_VW.ITEM_TYPE_CODE  IN('STANDARD', 'CONFIG')
			or (Master_view_VW.ITEM_TYPE_CODE = 'MODEL' AND Lkp_OE_ORDER_LINES_ALL_Config_VW.TOP_MODEL_LINE_ID is null))
		then 'N'
  else 
       'Y'
end as BACKLOG_FLAG,
case
when Master_view_VW.ITEM_TYPE_CODE  IN('STANDARD', 'CONFIG','SERVICE') and Master_view_VW.ORDER_HEADER_STATUS = 'SHIPPED' AND Master_view_VW.FOB_POINT_CODE = 'FOB DESTINATION'
then 'Y'
else  'N'
end as FOB_FLAG,
case
when Master_view_VW.ORDER_HEADER_STATUS in('SHIPPED', 'CLOSED', 'INVOICED') and Master_view_VW.ITEM_TYPE_CODE in('STANDARD', 'CONFIG', 'SERVICE')
then 'Y'
else 'N'
end as SALES_FLAG,
Master_view_VW.NULL_FIELD as EXTENDED_VALUE12,
Lkp_OE_ORDER_LINES_ALL_Price_VW.SALES_PRICE,
Master_view_VW.SALES_CHANNEL_CODE,
Master_view_VW.CUSTOMER_PO_LINE_NUMBER,
Master_view_VW.NULL_FIELD,
Master_view_VW.NULL_FIELD,
Master_view_VW.NULL_FIELD,
Master_view_VW.NULL_FIELD,
 Lkp_Trx_Extended_VW.TRX_NUMBER as INVOICE_NUMBER, 
Lkp_Trx_Extended_VW.TRX_DATE as INVOICE_DATE,
T35.PARTY_NAME  as CUSTOM_FIELD5,
'Y' as CUSTOM_FIELD6,
 Lkp_FND_Complexity_VW.ORDER_HEADER_ID as CUSTOM_FIELD7, 
Lkp_FND_Complexity_VW.Req_Eng as CUSTOM_FIELD8,
Master_view_VW.NULL_FIELD as CUSTOM_FIELD9,
Master_view_VW.NULL_FIELD as CUSTOM_FIELD10,
	 Lkp_Cst_VW.MATERIAL_COST  as MATERIAL_COST_TYPE_1000,
Lkp_Cst_VW.RESOURCE_COST as RESOURCE_COST_TYPE_1000,
Lkp_Cst_VW.OUTSIDE_PROCESSING_COST as OSP_COST_TYPE_1000, 
Lkp_Cst_VW.OVERHEAD_COST as OVERHEAD_COST_TYPE_1000,
Master_view_VW.QUOTE_NUMBER,
Master_view_VW.QUOTE_DATE,
Master_view_VW.TURBINE_NUMBER,
 Lkp_Job_Date_VW.JOB_CREATION_DATE as WIP_JOB_CREATION_DATE,
Lkp_Job_Date_VW.JOB_COMPLETED_DATE as WIP_JOB_COMPLETION_DATE,
Lkp_Job_Date_VW.JOB_SCHEDULED_COMPLETION_DATE as WIP_JOB_SCHEDULED_DATE,
 Lkp_User_VW.FULL_NAME as OPERATOR,
 UNIT_LIST_PRICE,
 Master_view_VW.ORDER_CREATION_DATE,
 Master_view_VW.NULL_FIELD as test3,
Lkp_DESCRIPTION_VW.FLEX_VALUE as PD_SALESREP_CODE,
Lkp_DESCRIPTION_VW.DESCRIPTION as PD_SALESREP_DESC,
Lkp_Itemwarehouse_VW.ATTRIBUTE10 as ATTRIBUTE10,
Lkp_Trx_all_VW.INVOICE_DATE as TRX_DATE,
nvl(Lkp_Mtl_Item_Type_C_VW.TOTAL_B_REVISIONS,Lkp_Revisions_B_VW.TOTAL_B_REVISIONS) as TOTAL_B_REVISIONS,
nvl(Lkp_Mtl_Item_Type_C_VW.REV_B_BOM_DATE,Lkp_Revisions_B_VW.REV_B_BOM_DATE) as REV_B_BOM_DATE,
Lkp_Trx_all_VW.INVOICE_NUMBER as CUSTOM_FIELD,
Master_view_VW.NULL_FIELD as test2,
Master_view_VW.NULL_FIELD as test1,
Lkp_FND_Req_VW.Req_Eng as REQ_ENG,
case
when Master_view_VW.CURRENCY_CODE='USD'
then 1
else Lkp_Conversion_VW.CONVERSION_RATE
End as INVOICE_USD_CONVERSION_RATE,
T51.CONVERSION_RATE as ITEM_CONVERSION_RATE,
'SPARK_IQP_ORDER_DELIVERY_FACT' as currentuser,
current_timestamp as updatetime,
nvl(Master_view_VW.WND_CARRIER_ID,-1),
nvl(Master_view_VW.ORG_ID,-1),
Master_view_VW.INVENTORY_ITEM_ID,
Master_view_VW.SHIP_FROM_ORG_ID,
Master_view_VW.SHIP_TO_ORG_ID,
Master_view_VW.INVOICE_TO_ORG_ID,
Master_view_VW.ORGANIZATION_ID
from Master_view_VW left join Lkp_FREIGHT_TERMS_VW T5 on(Master_view_VW.WSH_SHIP_METHOD_CODE=T5.LOOKUP_CODE and  T5.LOOKUP_TYPE = 'SHIP_METHOD') 
left outer join
 Lkp_FREIGHT_TERMS_VW T72 on(Master_view_VW.SHIP_METHOD_CODE=T72.LOOKUP_CODE and  T72.LOOKUP_TYPE = 'SHIP_METHOD') left outer join
Lkp_FREIGHT_TERMS_VW on(Master_view_VW.FREIGHT_TERMS=Lkp_FREIGHT_TERMS_VW.LOOKUP_CODE and Lkp_FREIGHT_TERMS_VW.LOOKUP_TYPE = 'FREIGHT_TERMS' and Lkp_FREIGHT_TERMS_VW.ENABLED_FLAG = 'Y') 
left outer join
Lkp_FREIGHT_TERMS_VW T7 on( Master_view_VW.FOB_CODE=T7.LOOKUP_CODE and T7.LOOKUP_TYPE = 'FOB' ) 
left outer join 
Lkp_OE_TRANSACTION_TYPES_TL_all_VW on(Master_view_VW.ORDER_TYPE_ID=Lkp_OE_TRANSACTION_TYPES_TL_all_VW.TRANSACTION_TYPE_ID)
left outer join
 Lkp_Source_name_VW on(Master_view_VW.ORDER_SOURCE_ID=Lkp_Source_name_VW.ORDER_SOURCE_ID)
								   left outer join Lkp_ORG_UNIT_NAME_VW on(Lkp_ORG_UNIT_NAME_VW.ORGANIZATION_ID = Master_view_VW.SHIP_FROM_ORG_ID) left outer join
								   Lkp_OE_ORDER_LINES_ALL_Sum_VW on (Lkp_OE_ORDER_LINES_ALL_Sum_VW.HEADER_ID = Master_view_VW.ORDER_HEADER_ID AND Lkp_OE_ORDER_LINES_ALL_Sum_VW.LINE_NUMBER = coalesce(cast(floor(Master_view_VW.LINE_NUMBER_STR) as string),'0') AND Lkp_OE_ORDER_LINES_ALL_Sum_VW.SHIPMENT_NUMBER =Master_view_VW.SHIPMENT_NUMBER) left outer join
Lkp_DESCRIPTION_VW on(Lkp_DESCRIPTION_VW.FLEX_VALUE =Master_view_VW.ATTRIBUTE15) 
left outer join Lkp_Job_Date_VW on(Lkp_Job_Date_VW.ORDER_LINE_ID=Master_view_VW.ORDER_LINE_ID)
left join Lkp_OE_ORDER_HOLDS_ALL_all_VW on(Lkp_OE_ORDER_HOLDS_ALL_all_VW.HEADER_ID =Master_view_VW.ORDER_HEADER_ID and Lkp_OE_ORDER_HOLDS_ALL_all_VW.LINE_ID=Master_view_VW.ORDER_LINE_ID) left outer join
Lkp_Customer_dim_Party_VW on(Lkp_Customer_dim_Party_VW.CUST_ACCOUNT_ID = Master_view_VW.SOLD_TO_ORG_ID) left outer join
Lkp_Customer_dim_Cust_id_VW on(Lkp_Customer_dim_Cust_id_VW.CUST_SITE_USES_SITE_USE_ID =Master_view_VW.SOLD_TO_SITE_USE_ID) left outer join
Lkp_Customer_dim_Cust_id_VW T29 on(T29.CUST_SITE_USES_SITE_USE_ID = Master_view_VW.END_CUSTOMER_SITE_USE_ID ) left outer join
Lkp_TRANSACTION_TYPE_ID_VW on(Lkp_TRANSACTION_TYPE_ID_VW.TRANSACTION_TYPE_ID =Master_view_VW.ORDER_TYPE_ID) left outer join
Lkp_Customer_dim_Party_VW Lkp_Customer_dim_Number_VW on(Lkp_Customer_dim_Number_VW.CUST_ACCOUNT_ID =Master_view_VW.END_CUSTOMER_ID) left outer join
Lkp_Trx_all_VW on(Lkp_Trx_all_VW.order_line_id=Master_view_VW.ORDER_LINE_ID) left outer join
Lkp_OE_ORDER_LINES_ALL_Config_VW on(Lkp_OE_ORDER_LINES_ALL_Config_VW.TOP_MODEL_LINE_ID = Master_view_VW.ORDER_LINE_ID) left outer join
Lkp_OE_ORDER_LINES_ALL_Price_VW on (Lkp_OE_ORDER_LINES_ALL_Price_VW.LINE_NUMBER = Master_view_VW.LINE_NUMBER_STR 
                                    AND Lkp_OE_ORDER_LINES_ALL_Price_VW.SHIPMENT_NUMBER = Master_view_VW.SHIPMENT_NUMBER 
                                    AND Lkp_OE_ORDER_LINES_ALL_Price_VW.SERVICE_NUMBER = case
                                              when Master_view_VW.SERVICE_NUMBER is null
                                              then 0
                                              else Master_view_VW.SERVICE_NUMBER
                                              end
                     AND Lkp_OE_ORDER_LINES_ALL_Price_VW.HEADER_ID = Master_view_VW.ORDER_HEADER_ID)
					left outer join
Lkp_Trx_Extended_VW on(ltrim(rtrim(Lkp_Trx_Extended_VW.SALES_ORDER_NUMBER)) = ltrim(rtrim(coalesce(cast(floor(Master_view_VW.ORDER_NUMBER) as string),'0'))) and
                          ltrim(rtrim(Lkp_Trx_Extended_VW.SALES_ORDER_LINE)) = ltrim(rtrim(coalesce(cast(floor(Master_view_VW.LINE_NUMBER_STR) as string),'0'))) and
                          ltrim(rtrim(Lkp_Trx_Extended_VW.SHIPMENT_NUMBER)) = ltrim(rtrim(coalesce(cast(floor(Master_view_VW.SHIPMENT_NUMBER) as string),'0'))) and 
                           ltrim(rtrim(Lkp_Trx_Extended_VW.SERVICE_NUMBER)) =   ltrim(rtrim(coalesce(cast(floor(Master_view_VW.SERVICE_NUMBER) as string),'0')))               ) 
									  														
left outer join
Lkp_Customer_dim_Cust_id_VW T35 on(T35.CUST_SITE_USES_SITE_USE_ID =Master_view_VW.SHIP_TO_ORG_ID ) 
left outer join
 Lkp_FND_Complexity_VW on(Lkp_FND_Complexity_VW.ORDER_HEADER_ID = Master_view_VW.ORDER_HEADER_ID )
 left outer join
 Lkp_Cst_VW on(Lkp_Cst_VW.INVENTORY_ITEM_ID = Master_view_VW.INVENTORY_ITEM_ID AND Lkp_Cst_VW.ORGANIZATION_ID = Master_view_VW.SHIP_FROM_ORG_ID)
left outer join
Lkp_User_VW on(Lkp_User_VW.PERSON_ID = Master_view_VW.CREATED_BY)
left outer join
Lkp_Itemwarehouse_VW on(Lkp_Itemwarehouse_VW.INVENTORY_ITEM_ID = cast(Master_view_VW.INVENTORY_ITEM_ID as string) AND Lkp_Itemwarehouse_VW.ORGANIZATION_ID = cast(Master_view_VW.SHIP_FROM_ORG_ID as string))
left outer join
Lkp_Mtl_Item_Type_C_VW on(Lkp_Mtl_Item_Type_C_VW.INVENTORY_ITEM_ID = Master_view_VW.INVENTORY_ITEM_ID AND Lkp_Mtl_Item_Type_C_VW.ORGANIZATION_ID = Master_view_VW.ORGANIZATION_ID)
left outer join
Lkp_Revisions_B_VW on(Lkp_Revisions_B_VW.INVENTORY_ITEM_ID = Master_view_VW.INVENTORY_ITEM_ID AND Lkp_Revisions_B_VW.ORGANIZATION_ID = Master_view_VW.ORGANIZATION_ID)
left outer join
Lkp_FND_Req_VW on(Lkp_FND_Req_VW.ORDER_LINE_ID = cast(Master_view_VW.ORDER_LINE_ID as string))
  left outer join
  Lkp_Conversion_VW on(to_date(Lkp_Conversion_VW.CONVERSION_DATE) =to_date(Lkp_Trx_all_VW.INVOICE_DATE) AND Lkp_Conversion_VW.FROM_CURRENCY = Master_view_VW.CURRENCY_CODE and  Lkp_Conversion_VW.TO_CURRENCY ='USD')
left outer join
Lkp_Conversion_VW T51 on(to_date(T51.CONVERSION_DATE) = to_date(Lkp_Trx_all_VW.INVOICE_DATE) AND T51.TO_CURRENCY ='USD' AND T51.FROM_CURRENCY ='CAD')
left outer join
Lkp_Conversion_VW T53 on(to_date(T53.CONVERSION_DATE )= to_date(Master_view_VW.BOOKED_DATE) and T53.FROM_CURRENCY = Master_view_VW.TRANSACTION_CURRENCY and T53.TO_CURRENCY = 'USD')
""")

val finalResultToWriteWithPartition = finalResultToWrite.repartition(10)
finalResultToWriteWithPartition.createOrReplaceTempView("internalFinalResultTable") 
if (deBug) Utils.printLog(deBug,tableName + ": Writing to Table Count: " + formatter.format(finalResultToWrite.count))
val sql = s"""Insert overwrite table $tableName select * from internalFinalResultTable"""
spark.sql(sql)


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
