import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.DataFrame
 
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import java.io.File
 
 
object o_iqp_booking_fact_bravo
{

  def main(args: Array[String])={
 

    val warehouseLocation = new File("spark-warehouse").getAbsolutePath
 
    val spark = SparkSession.builder
      .appName("booking-app")
      .config("spark.some.config.option", "config-value")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()

spark.conf.set("spark.sql.crossJoin.enabled", true) 
spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
spark.sql("set hive.exec.dynamic.partition=true")
spark.udf.register("getProductPlatform", (TIER2:String,TIER3:String,ITEM_NUMBER:String,SUB_PRODUCT_LINE_ITEM:String,SUB_PRODUCT_LINE:String,PRODUCT_NUMBER:Int,PRODUCT_PLATFORM:String,PRODUCT_PLATFORM_ITEM:String,ORGANIZATION_CODE:String) => { 
val arr1 = Array("GRID AUTOMATION","XD PRIMARY EQUIPMENT","PEA","PTR","AIS","CAPACITORS","ACS")
val arr2 = Array("C55","C57","C58","C81","C82","C83","C50","C71","C86")
val arr3=  Array("XD PRIMARY EQUIPMENT","PEA","PTR","AIS","CAPACITORS","ACS")
val v_product_platform = if( ITEM_NUMBER == null)  PRODUCT_PLATFORM_ITEM else PRODUCT_PLATFORM
val prod_value = if (PRODUCT_NUMBER != 0) "GATEWAYS/RTUS/SAS"  else "ACCESSORIES" 

val exp1 = if( ITEM_NUMBER == null && TIER3 == "PROTECTION & CONTROL")  prod_value else PRODUCT_PLATFORM_ITEM
val exp2 = if(ITEM_NUMBER == null) exp1  else PRODUCT_PLATFORM
val exp3 = if(ITEM_NUMBER == null ) SUB_PRODUCT_LINE_ITEM  else SUB_PRODUCT_LINE
val exp4 =if(TIER2=="GRID AUTOMATION" && TIER3=="M&D")  exp3   else  exp2
val v_product_platform_1 =if(TIER2 == "GRID AUTOMATION") exp4    else if ( arr3.contains(TIER2)) v_product_platform  ; else PRODUCT_PLATFORM
val exp5 = if (arr2.contains(ORGANIZATION_CODE) == false)   "T&D Products" else v_product_platform_1
val result =    if (arr1.contains(TIER2))  v_product_platform_1    else exp5 
result
})

spark.udf.register("getProductSegment", (TIER2:String,ITEM_NUMBER_PLR:String,PRODUCT_SEGMENT_ITEM:String,PRODUCT_SEGMENT_PLR:String,ORGANIZATION_CODE:String) => { 
val arr1 = Array("GRID AUTOMATION","XD PRIMARY EQUIPMENT","PEA","PTR","AIS","CAPACITORS","ACS")
val arr2 = Array("C55","C57","C58","C81","C82","C83","C50","C71","C86")

val v_product_segment_1 = if(ITEM_NUMBER_PLR == null)  Option(PRODUCT_SEGMENT_ITEM).getOrElse("").toUpperCase  else Option(PRODUCT_SEGMENT_PLR).getOrElse("").toUpperCase 
val expr2 = if (arr2.contains(ORGANIZATION_CODE) == false)  "T&D Products"  else  v_product_segment_1
val expr1 = if (arr1.contains(TIER2)) v_product_segment_1 else expr2
expr1
})

spark.udf.register("getPRODUCTCLASSGROUP", (TIER2:String,TIER3:String,ITEM_NUMBER_PLR:String,PRODUCT_GROUP_ITEM:String,PRODUCT_NUMBER:Int,PRODUCT_GROUP_PLR:String,PRODUCTCLASS_ITEM:String,PRODUCTCLASS_PLR:String,PNL:String,Operational_Department:String) => { 
val arr1 = Array("XD PRIMARY EQUIPMENT","PEA","PTR","AIS","CAPACITORS","ACS")

val v_PRODUCT_GROUP_PD = if(ITEM_NUMBER_PLR == null)  Option(PRODUCT_GROUP_ITEM).getOrElse("").toUpperCase   else  Option(PRODUCT_GROUP_PLR).getOrElse("").toUpperCase
val expr6 = if(PRODUCT_NUMBER != 0)  "GATEWAYS PARTS"  else  "PARTS"
val expr5 = if(ITEM_NUMBER_PLR == null && TIER3 == "PROTECTION & CONTROL")  expr6  else Option(PRODUCT_GROUP_ITEM).getOrElse("").toUpperCase 
val expr4 = if(ITEM_NUMBER_PLR == null)  expr5  else  Option(PRODUCT_GROUP_PLR).getOrElse("").toUpperCase
val expr2 = if(ITEM_NUMBER_PLR == null)  Option(PRODUCTCLASS_ITEM).getOrElse("").toUpperCase  else Option(PRODUCTCLASS_PLR).getOrElse("").toUpperCase
val expr3 = if (TIER2 == "GRID AUTOMATION" && TIER3 == "M&D" && Operational_Department == "SVC")  "SERVICES"  else  expr4
val v_PRODUCT_GROUP_GA = if (TIER2 == "GRID AUTOMATION" && TIER3 == "COMMS" && PNL == "MDS")  expr2  else  expr3
val expr1 = if (TIER2 == "GRID AUTOMATION") v_PRODUCT_GROUP_GA   else if  (arr1.contains(TIER2))  v_PRODUCT_GROUP_PD    else  Option(PRODUCT_GROUP_PLR).getOrElse("").toUpperCase
expr1
})

spark.udf.register("getMANAGEMENT_ENTITY", (ORGANIZATION_CODE:String,MANAGEMENT_ENTITY:String,MANAGEMENT_ENTITY_PLR:String,LKP_ME_REF:String) => { 
val arr1 = Array("C47", "C55", "C57", "C58","C81","C82","C83","C86")

val expr1 = if (LKP_ME_REF == null)  MANAGEMENT_ENTITY  else  LKP_ME_REF
val V_MANAGEMENT_ENTITY_PLR = if (MANAGEMENT_ENTITY_PLR == null || MANAGEMENT_ENTITY_PLR == "" || MANAGEMENT_ENTITY_PLR == "0")  expr1  else  MANAGEMENT_ENTITY_PLR
val res = if (arr1.contains(ORGANIZATION_CODE))  V_MANAGEMENT_ENTITY_PLR  else  MANAGEMENT_ENTITY
res
})


val s2 = spark.sql("""select
ATTRIBUTE13,
header_id,
LINE_ID,Org_Id,SHIPMENT_NUMBER,UNIT_SELLING_PRICE,item_type_code,LINE_NUMBER
from g00103.oe_order_lines_all""")
s2.createOrReplaceTempView("df_oe_order_lines_all")
spark.sql("cache table df_oe_order_lines_all")

val l19 = spark.sql("""select PRODUCTCLASS,
PRODUCTLINE,
PRODUCT_PLATFORM,
PRODUCT_SEGMENT,
SUB_PRODUCT_LINE,
PRODUCT_GROUP,
ITEM_NUMBER,
RECORD_INSERTED_BY,
BUSINESSSEGMENT,
MANAGEMENT_ENTITY,
INVENTORY_ITEM_ID,
PLANTCODE
from g00103.IQP_LEGACY_CATALOG_LIST""")

l19.createOrReplaceTempView("lkp_iqp_legacy_catalog_list_base")
spark.sql("CACHE TABLE lkp_iqp_legacy_catalog_list_base")

val l1 = spark.sql("""select CUST_SITE_USES_SITE_USE_ID,
LOCATIONS_ADDRESS1,
LOCATIONS_ADDRESS2,
LOCATIONS_ADDRESS3,
LOCATIONS_ADDRESS4,
LOC_COUNTRY,
LOC_STATE,
CUSTOMER_GROUP,
CUSTOMER_CLASSIFICATION,
BILL_TO_REGION,
SHIP_TO_REGION,
BILL_TO_SUB_REGION,
SHIP_TO_SUB_REGION,
PARTY_NUMBER,
PARTY_NAME,
LOC_CITY,
LOC_COUNTY,
LOC_POSTAL_CODE,
LOC_PROVINCE,
PARTY_SITE_NUMBER,
CUST_ACCT_SITES_ORG_ID
from g00103.IQP_CUSTOMER_DIM""")
l1.createOrReplaceTempView("lkp_customer_base")

val l13 = spark.sql("""select STATE_CODE,
STATE_NAME,
REGION_NAME,
COUNTRY_CODE,
SUB_REGION_NAME,
COUNTRY_NAME
from g00103.IQP_GEOGRAPHY_DIM""")

l13.createOrReplaceTempView("lkp_geography_base")
spark.sql("CACHE TABLE lkp_geography_base")

val l14 = spark.sql("""select STATE_CODE,
max(STATE_NAME) as STATE_NAME
from lkp_geography_base
group by STATE_CODE""")

l14.createOrReplaceTempView("lkp_state")

val l15 = spark.sql("""SELECT max(a.REGION_NAME)              as REGION_NAME,
       a.COUNTRY_CODE             as COUNTRY_CODE,
       max(a.SUB_REGION_NAME)          as SUB_REGION_NAME,
       a.STATE_CODE               as STATE_CODE
 FROM lkp_geography_base a
WHERE REGION_NAME IS NOT NULL
group by COUNTRY_CODE,STATE_CODE""")

l15.createOrReplaceTempView("lkp_geography_country_state")

val l16 = spark.sql("""SELECT max(a.REGION_NAME)              as REGION_NAME,
       a.COUNTRY_CODE             as COUNTRY_CODE,
       max(a.SUB_REGION_NAME)          as SUB_REGION_NAME,
           max(COUNTRY_NAME)                            as COUNTRY_NAME
  FROM lkp_geography_base a
WHERE REGION_NAME IS NOT NULL
group by COUNTRY_CODE""")

l16.createOrReplaceTempView("lkp_geography_country")

val l2 = spark.sql("""select CUST_SITE_USES_SITE_USE_ID,
max(BILL_TO_REGION) as BILL_TO_REGION,
max(BILL_TO_SUB_REGION) as BILL_TO_SUB_REGION
from lkp_customer_base
group by CUST_SITE_USES_SITE_USE_ID""")

l2.createOrReplaceTempView("lkp_bill_to_customer")

val l25 = spark.sql("""select TIER2 as TIER2,
TIER3 as TIER3,
PNL as PNL,
PLANTCODE_ID
from g00103.IQP_BUSINESS_HIER_DIM""")

l25.createOrReplaceTempView("lkp_business_heir")

val l30 = spark.sql("""SELECT 
  max(POH.SEGMENT1) as  PO_NUM,
OEL.LINE_ID as LINE_ID
FROM 
g00103.po_headers_all POH ,
df_oe_order_lines_all OEL ,
  g00103.oe_drop_ship_sources OED 
WHERE 
 OEL.LINE_ID                 = OED.LINE_ID
AND POH.PO_HEADER_ID            = OED.PO_HEADER_ID
AND Oel.Org_Id                 IN (2893,3113,872)
group by oel.LINE_ID""")

l30.createOrReplaceTempView("lkp_oracle_po_number")

val l27 = spark.sql("""select PNL,
PRODUCT_PLATFORM,
SUB_BUSINESS,
BUSINESS_SEGMENT
from 
g00103.IQP_SUB_BUSINESS_DIM""")

l27.createOrReplaceTempView("lkp_iqp_sub_business_base")
spark.sql("CACHE TABLE lkp_iqp_sub_business_base")

val l28 = spark.sql("""select PNL,
PRODUCT_PLATFORM,
SUB_BUSINESS
from lkp_iqp_sub_business_base""")

l28.createOrReplaceTempView("lkp_iqp_sub_business")

val l29 = spark.sql("""select PNL,
PRODUCT_PLATFORM,
BUSINESS_SEGMENT
from lkp_iqp_sub_business_base""")

l29.createOrReplaceTempView("lkp_iqp_sub_business1")

val l8 = spark.sql("""SELECT max(MTL_CATEGORIES.SEGMENT1)               as SEGMENT1,
       max(MTL_CATEGORIES.SEGMENT2)               as SEGMENT2,
       max(MTL_CATEGORIES.SEGMENT3)               as SEGMENT3,
       max(MTL_CATEGORIES.SEGMENT4)               as SEGMENT4,
       max(MTL_CATEGORIES.SEGMENT5)               as SEGMENT5,
       max(MTL_CATEGORIES.SEGMENT6)               as SEGMENT6,
       max(MTL_CATEGORIES.SEGMENT7)               as SEGMENT7,
       max(MTL_CATEGORIES.SEGMENT8)               as SEGMENT8,
       MTL_ITEM_CATEGORIES.INVENTORY_ITEM_ID AS INVENTORY_ITEM_ID,
       MTL_ITEM_CATEGORIES.ORGANIZATION_ID   AS WAREHOUSE_ID
  FROM g00103.MTL_CATEGORIES_B      MTL_CATEGORIES,
       g00103.MTL_ITEM_CATEGORIES   MTL_ITEM_CATEGORIES,
       g00103.MTL_CATEGORY_SETS_tl  MTL_CATEGORY_SETS,
       g00103.HR_ORGANIZATION_UNITS IO
WHERE MTL_CATEGORIES.CATEGORY_ID = MTL_ITEM_CATEGORIES.CATEGORY_ID
   AND MTL_ITEM_CATEGORIES.CATEGORY_SET_ID =
       MTL_CATEGORY_SETS.CATEGORY_SET_ID
   AND MTL_ITEM_CATEGORIES.ORGANIZATION_ID = IO.ORGANIZATION_ID
   AND MTL_CATEGORY_SETS.CATEGORY_SET_NAME =
       (case when IO.NAME LIKE 'IO Multilin%' then 'Product Line Category' when
        IO.NAME LIKE 'IO Somersworth%' then 'Product Line Accounting' when IO.NAME LIKE 'IO Rochester%' then 'ROC Product Line Category' else
        'Order Entry Category' end)
        group by INVENTORY_ITEM_ID, MTL_ITEM_CATEGORIES.ORGANIZATION_ID""")
        
        l8.createOrReplaceTempView("lkp_categoris")

val l9 = spark.sql("""SELECT DR.CONVERSION_DATE AS CONVERSION_DATE,
                DR.FROM_CURRENCY   AS FROM_CURRENCY,
                nvl(DR.CONVERSION_RATE,1) AS CONVERSION_RATE
  FROM g00103.GL_DAILY_RATES DR
WHERE DR.CONVERSION_TYPE = '1000'
   AND DR.TO_CURRENCY = 'USD'""")
   
l9.createOrReplaceTempView("lkp_conversion")

val l12 = spark.sql("""select ORDER_NUMBER,
max(ORD_HDR_ORIG_SYS_DOCUMENT_REF) as ORD_HDR_ORIG_SYS_DOCUMENT_REF
from g00103.IQP_ORDER_MGMT_FACT
where order_number is not null
group by ORDER_NUMBER""")

l12.createOrReplaceTempView("lkp_orig_doc_ref")

val l11 = spark.sql("""select max(SEGMENT5) as SEGMENT5,
CODE_COMBINATION_ID
from g00103.IQP_GL_CODE_COMBINATION_DIM group by CODE_COMBINATION_ID""")

l11.createOrReplaceTempView("lkp_gl_code_combination")
 
 val l5 = spark.sql("""select ORGANIZATION_ID,
ORG_UNIT_NAME ORG_UNIT_NAME,
ORGANIZATION_CODE ORGANIZATION_CODE
from g00103.IQP_ORGANIZATION_DIM""")

l5.createOrReplaceTempView("lkp_organization")

val l10 = spark.sql("""select ic.inventory_item_id AS INVENTORY_ITEM_ID,
       ic.organization_id AS ORGANIZATION_ID,
       nvl(ic.material_cost, 0) AS MATERIAL_COST,
       nvl(ic.material_overhead_cost, 0) AS MATERIAL_OVERHEAD_COST,
       nvl(ic.resource_cost, 0) AS LABOUR_COST,
       nvl(ic.overhead_cost, 0) AS OVERHEAD_COST,
       nvl(ic.outside_processing_cost, 0) AS OSP_COST
  from g00103.cst_item_costs ic
where ic.cost_type_id = '2'""")

l10.createOrReplaceTempView("lkp_avg_cost")
 
val l4 = spark.sql("""SELECT
           max(a.PARTY_NUMBER)       as PARTY_NUMBER,
           max(a.PARTY_NAME)         as PARTY_NAME,
           max(a.LOCATIONS_ADDRESS1) as LOCATIONS_ADDRESS1,
           max(a.LOCATIONS_ADDRESS2) as LOCATIONS_ADDRESS2,
           max(a.LOCATIONS_ADDRESS3) as LOCATIONS_ADDRESS3,
           max(a.LOCATIONS_ADDRESS4) as LOCATIONS_ADDRESS4,
           max(a.LOC_CITY)           as LOC_CITY,
           max(a.LOC_STATE)          as LOC_STATE,
           max(a.LOC_COUNTRY)        as LOC_COUNTRY,
           max(a.LOC_COUNTY)         as LOC_COUNTY,
           max(a.LOC_POSTAL_CODE)    as LOC_POSTAL_CODE,
           max(a.LOC_PROVINCE)       as LOC_PROVINCE,
           a.PARTY_SITE_NUMBER  as PARTY_SITE_NUMBER
      FROM lkp_customer_base a
WHERE a.CUST_ACCT_SITES_ORG_ID in (872, 2893, 3113) 
group by a.PARTY_SITE_NUMBER""")

l4.createOrReplaceTempView("lkp_sold_to_customer11")

val l21 = spark.sql("""SELECT PRODUCTCLASS as PRODUCTCLASS,
       BUSINESSSEGMENT as BUSINESSSEGMENT,
       PRODUCTLINE as PRODUCTLINE,
       PRODUCT_PLATFORM as PRODUCT_PLATFORM,
       PRODUCT_SEGMENT as PRODUCT_SEGMENT,
       MANAGEMENT_ENTITY as MANAGEMENT_ENTITY,
       INVENTORY_ITEM_ID as INVENTORY_ITEM_ID,
           ITEM_NUMBER as ITEM_NUMBER,
           PRODUCT_GROUP as PRODUCT_GROUP,
           SUB_PRODUCT_LINE as SUB_PRODUCT_LINE,
       substr(PLANTCODE, 1,4) as PLANTCODE
  FROM lkp_iqp_legacy_catalog_list_base""")
l21.createOrReplaceTempView("lkp_iqp_legacy_catalog_list_plr")

val l18 = spark.sql("""select ORG_CODE, MANAGEMENT_ENTITY as MANAGEMENT_ENTITY
from
g00103.IQP_ME_MAPPING_REF""")

l18.createOrReplaceTempView("lkp_me_ref")

val l20 = spark.sql("""SELECT max(PRODUCTCLASS) as PRODUCTCLASS,
       max(PRODUCTLINE) as PRODUCT_LINE,
       max(PRODUCT_PLATFORM) as PRODUCT_PLATFORM,
       max(PRODUCT_SEGMENT) as PRODUCT_SEGMENT,
       max(SUB_PRODUCT_LINE) AS SUB_PRODUCT_LINE,
      max(PRODUCT_GROUP) AS PRODUCT_GROUP,
ITEM_NUMBER AS ITEM_NUMBER
  FROM lkp_iqp_legacy_catalog_list_base
WHERE RECORD_INSERTED_BY = 'SPARK_o_g00103.IQP_legacy_catalog_list'
group by ITEM_NUMBER""")

l20.createOrReplaceTempView("lkp_iqp_legacy_catalog_list_item")

val l3 = spark.sql("""select CUST_SITE_USES_SITE_USE_ID,
max(LOCATIONS_ADDRESS1) as LOCATIONS_ADDRESS1,
max(LOCATIONS_ADDRESS2) as LOCATIONS_ADDRESS2,
max(LOCATIONS_ADDRESS3) as LOCATIONS_ADDRESS3,
max(LOCATIONS_ADDRESS4) as LOCATIONS_ADDRESS4,
max(LOC_COUNTRY) as LOC_COUNTRY,
max(LOC_STATE) as LOC_STATE,
max(CUSTOMER_GROUP) as CUSTOMER_GROUP,
max(LOC_POSTAL_CODE) as LOC_POSTAL_CODE,
max(CUSTOMER_CLASSIFICATION) as CUSTOMER_CLASSIFICATION,
max(Ship_TO_REGION) as Ship_TO_REGION,
max(SHIP_TO_SUB_REGION) as SHIP_TO_SUB_REGION
from lkp_customer_base
group by CUST_SITE_USES_SITE_USE_ID""")
l3.createOrReplaceTempView("lkp_ship_to_customer")

val l23 = spark.sql("""select
HEADER_ID,
LINE_ID,
SHIPMENT_NUMBER
from df_oe_order_lines_all""")

l23.createOrReplaceTempView("lkp_iqp_oe_order_lines_shipment_no")

val l24 = spark.sql("""SELECT distinct 
  A.HEADER_ID               AS HEADER_ID,
  A.LINE_NUMBER             AS LINE_NUMBER,
  A.SHIPMENT_NUMBER         AS SHIPMENT_NUMBER,
  SUM(A.UNIT_SELLING_PRICE) OVER(PARTITION BY A.HEADER_ID, A.LINE_NUMBER, A.SHIPMENT_NUMBER) AS UNIT_SELLING_PRICE
FROM  df_oe_order_lines_all A
WHERE A.ITEM_TYPE_CODE <> 'STANDARD' 
AND A.UNIT_SELLING_PRICE != 0""")

l24.createOrReplaceTempView("lkp_config_unit_price")

val l31 = spark.sql("""SELECT b.NET_QUANTITY             as NEG_NET_QUANTITY,
       b.UNIT_SELLING_PRICE       as NEG_UNIT_SELLING_PRICE,
       b.NET_AMOUNT_FUNC_CURRENCY as NEG_NET_AMOUNT_FUNC_CURRENCY,
       b.MATERIAL_COST            as NEG_MATERIAL_COST,
       b.MATARIAL_OVHD_COST       as NEG_MATARIAL_OVHD_COST,
       b.LABOR_COST               as NEG_LABOR_COST,
       b.OVERHEAD_COST            as NEG_OVERHEAD_COST,
       b.OSP_COST                 as NEG_OSP_COST,
       b.COMMISSIONS              as NEG_COMMISSIONS,
       b.FREIGHT                  as NEG_FREIGHT,
       b.NET_AMOUNT_USD           as NEG_NET_AMOUNT_USD,
       b.MATERIAL_COST_USD        as NEG_MATERIAL_COST_USD,
       b.MATARIAL_OVHD_COST_USD   as NEG_MATARIAL_OVHD_COST_USD,
       b.LABOUR_COST_USD          as NEG_LABOUR_COST_USD,
       b.OVERHEAD_COST_USD        as NEG_OVERHEAD_COST_USD,
       b.OSP_COST_USD             as NEG_OSP_COST_USD,
       b.FREIGHT_COST_USD         as NEG_FREIGHT_COST_USD,
       b.MANAGEMENT_ENTITY        as NEG_MANAGEMENT_ENTITY,
       b.CUSTOMER_GROUP           as NEG_CUSTOMER_GROUP,
       b.SHIP_FROM_WAREHOUSE_CODE as NEG_SHIP_FROM_WAREHOUSE_CODE,
       b.ORDER_HEADER_ID          as NEG_ORDER_HEADER_ID,
       b.ORDER_LINE_ID            as NEG_ORDER_LINE_ID,
       b.ORDER_STATUS_CODE        as NEG_ORDER_STATUS_CODE,
       b.LINE_STATUS_CODE         as NEG_LINE_STATUS_CODE,
       b.CM_AMOUNT                         AS NEG_CM_AMOUNT,
  b.BURDENED_COST                         as NEG_BURDENED_COST,
           b.counter                               as NEG_counter

from g00103.iqp_order_booking_fact b inner join
(select concat_ws('-',cast(order_header_id as String),cast(order_line_id as String),cast(nvl(max(counter),'') as String)) ab
from g00103.iqp_order_booking_fact
where ACTUAL_ORDER_LINE_FLAG = 'Y' and record_created_by = 'm_IQP_ORDER_BOOKING_FACT_BRAVO' group by order_header_id,order_line_id) a 
on (concat_ws('-',cast(b.order_header_id as String),cast(b.order_line_id as String),cast(nvl(b.counter,'') as String))=a.ab) where b.record_created_by = 'm_IQP_ORDER_BOOKING_FACT_BRAVO'
""")
                  
l31.createOrReplaceTempView("lkp_target")
spark.sql("CACHE TABLE lkp_target")

 val l32 = spark.sql("""SELECT
       IQP_ORDER_BOOKING_FACT.ORDER_HEADER_ID   as CE_ORDER_HEADER_ID,
      IQP_ORDER_BOOKING_FACT.ORDER_LINE_ID     as CE_ORDER_LINE_ID
  FROM g00103.iqp_order_booking_fact IQP_ORDER_BOOKING_FACT
  WHERE CANCELLED_FLAG = 'Y'""")
  
l32.createOrReplaceTempView("lkp_cancellation_exists")

val final_query = spark.sql("""select 
omf.header_id as ORDER_HEADER_ID,
omf.line_id as ORDER_LINE_ID,
case when upper(case when upper(br.reporting_process) = 'PULSE REPORTING' then br.rejection_legend else il.inclusion_legend end) = 'PULSE REPORTING' then nvl(smd.order_number,omf.order_number) else omf.order_number end as order_number,
omf.LINE_NUMBER as LINE_NUMBER,
msi.segment1 Catlog_number,
msi.description Catlog_description,
omf.ord_hdr_cust_po_number as Customer_Po_Number,
hzca1.account_number Ship_to_Customer_number,
hzp1.party_name Ship_to_Customer_name,
CASE hzs.attribute4 WHEN 'IBS_1' THEN 'Within Division' WHEN 'IBS_2' THEN 'Within Group' WHEN 'IBS_3' THEN 'Within Other GE' WHEN 'IBS_4' THEN 'Within Other GE' WHEN 'IBS_5' THEN 'Within Other GE' WHEN 'NONIBS_1' THEN 'Within Division' WHEN 'NONIBS_2' THEN 'Within Group' WHEN 'NONIBS_3' THEN 'Within Other GE' WHEN 'NONIBS_4' THEN 'Within Other GE' WHEN 'External' THEN 'External' ELSE 'External' END Customer_group,
lkp_ship_to_customer.LOCATIONS_ADDRESS1 as SHIP_TO_ADDRESS_1,
lkp_ship_to_customer.LOCATIONS_ADDRESS2 as SHIP_TO_ADDRESS_2,
lkp_ship_to_customer.LOCATIONS_ADDRESS3 as SHIP_TO_ADDRESS_3,
lkp_ship_to_customer.LOCATIONS_ADDRESS4 as SHIP_TO_ADDRESS_4,
loc1.city ship_to_city,
nvl(lkp_state.STATE_NAME,lkp_ship_to_customer.LOC_STATE) as SHIP_TO_STATE,
nvl(lkp_geography_country.COUNTRY_NAME,lkp_ship_to_customer.LOC_COUNTRY) as SHIP_TO_COUNTRY,
lkp_ship_to_customer.LOC_POSTAL_CODE AS SHIP_TO_ZIP_CODE,
hzca.account_number bill_to_Customer_number,
hzp.party_name bill_to_Customer_name,
omf.INVOICE_TO_ADDRESS1 as BILL_TO_ADDRESS_1,
omf.INVOICE_TO_ADDRESS2 as BILL_TO_ADDRESS_2,
omf.INVOICE_TO_ADDRESS3 as BILL_TO_ADDRESS_3,
omf.INVOICE_TO_ADDRESS4 as BILL_TO_ADDRESS_4,
loc.city bill_to_city,
nvl(lkp_state.STATE_NAME,omf.INVOICE_TO_STATE) as BILL_TO_STATE,
nvl(lkp_geography_country.COUNTRY_NAME,omf.INVOICE_TO_COUNTRY) as BILL_TO_COUNTRY,
NULL as BILL_TO_ZIP_CODE,
lkp_sold_to_customer11.PARTY_NUMBER as SOLD_TO_CUSTOMER_NUMBER,
lkp_sold_to_customer11.PARTY_NAME as SOLD_TO_CUSTOMER_NAME,
lkp_sold_to_customer11.LOCATIONS_ADDRESS1 as SOLD_TO_ADDRESS_1,
lkp_sold_to_customer11.LOCATIONS_ADDRESS2 as SOLD_TO_ADDRESS_2,
lkp_sold_to_customer11.LOCATIONS_ADDRESS3 as SOLD_TO_ADDRESS_3,
lkp_sold_to_customer11.LOCATIONS_ADDRESS4 as SOLD_TO_ADDRESS_4,
lkp_sold_to_customer11.LOC_CITY as SOLD_TO_CITY,
lkp_sold_to_customer11.LOC_STATE as SOLD_TO_STATE,
lkp_sold_to_customer11.LOC_COUNTRY as SOLD_TO_COUNTRY,
lkp_sold_to_customer11.LOC_POSTAL_CODE as SOLD_TO_ZIP_CODE,
hzp1.PARTY_NAME AS END_USER,
TO_DATE(omf.ORD_LINE_REQUEST_DATE) customer_request_date,
omf.ord_hdr_creation_date as ORDER_RECEIPT_DATE,
omf.ordered_date as ORDERED_DATE,
omf.ordered_date as ORDER_ENTRY_DATE,
omf.booked_date as booked_date,
omf.promise_date as promise_date,
omf.schedule_ship_date as schedule_ship_date,
omf.actual_shipment_date as SHIPPED_DATE,
current_timestamp as Process_date,
omf.quote_date as quote_date,
case when lkp_organization.organization_code NOT IN('C55','C57','C58','C71','C81','C82','C83') 
then msi.segment1 
else (case when lkp_organization.ORGANIZATION_CODE in ('C55', 'C57', 'C58', 'C71','C81','C82','C83') 
                then case when lkp_iqp_legacy_catalog_list_plr.PRODUCTCLASS is null or trim(lkp_iqp_legacy_catalog_list_plr.PRODUCTCLASS) = '' 
                                        then 'OTHERS' 
                                        else lkp_iqp_legacy_catalog_list_plr.PRODUCTCLASS end 
                else msi.segment1 end) end as PRODUCT_CLASS_CODE,
omf.sales_channel_code as SALES_CHANNEL,
lkp_organization.ORG_UNIT_NAME as SHIP_FROM_WAREHOUSE_NAME,
lkp_organization.ORGANIZATION_CODE as SHIP_FROM_WAREHOUSE_CODE,
getMANAGEMENT_ENTITY(lkp_organization.ORGANIZATION_CODE,lkp_gl_code_combination.SEGMENT5,trim(lkp_iqp_legacy_catalog_list_plr.MANAGEMENT_ENTITY),lkp_me_ref.MANAGEMENT_ENTITY) as MANAGEMENT_ENTITY,
nvl(lkp_geography_country_state.REGION_NAME,lkp_geography_country.REGION_NAME) as REGION,
nvl(lkp_geography_country_state.SUB_REGION_NAME,lkp_geography_country.SUB_REGION_NAME) as SUBREGION,
omf.transactional_curr_code as CURRENCY_CODE,
(case when omf.line_category_code ='RETURN' then
nvl(omf.ordered_quantity, 0) * (-1) else
nvl(omf.ordered_quantity, 0) end) Net_Quantity,
omf.UNIT_SELLING_PRICE as UNIT_SELLING_PRICE,
case when omf.ITEM_TYPE_CODE='CONFIG' AND lkp_organization.ORGANIZATION_CODE = 'C71' 
then lkp_config_unit_price.UNIT_SELLING_PRICE*(case when omf.line_category_code ='RETURN' then nvl(omf.ordered_quantity, 0) * (-1) else nvl(omf.ordered_quantity, 0) end) else 
(case when omf.line_category_code = 'RETURN' then
              ROUND(NVL((omf.unit_selling_price * omf.ordered_quantity), 0),
                    2) * (-1) else
              ROUND(NVL((omf.unit_selling_price * omf.ordered_quantity), 0),
                    2) end) end as NET_AMOUNT_FUNC_CURRENCY,
lkp_avg_cost.MATERIAL_COST as MATERIAL_COST,
lkp_avg_cost.MATERIAL_OVERHEAD_COST as MATERIAL_OVERHEAD_COST,
lkp_avg_cost.LABOUR_COST as LABOUR_COST,
lkp_avg_cost.OVERHEAD_COST as OVERHEAD_COST,
lkp_avg_cost.OSP_COST as OSP_COST,
NULL as COMMISSION,
NULL as FREIGHT,
nvl(lkp_conversion.CONVERSION_RATE,1) as CONVERSION_RATE,
case when omf.ITEM_TYPE_CODE='CONFIG' AND lkp_organization.ORGANIZATION_CODE = 'C71' then lkp_config_unit_price.UNIT_SELLING_PRICE*(case when omf.line_category_code ='RETURN' then
              nvl(omf.ordered_quantity, 0) * (-1) else
              nvl(omf.ordered_quantity, 0) end)*nvl(lkp_conversion.CONVERSION_RATE,1) else (case when omf.line_category_code ='RETURN' then
              nvl(omf.ordered_quantity, 0) * (-1) else
              nvl(omf.ordered_quantity, 0) end)*omf.UNIT_SELLING_PRICE*nvl(lkp_conversion.CONVERSION_RATE,1) end as NET_AMOUNT_USD,
lkp_avg_cost.MATERIAL_COST*nvl(lkp_conversion.CONVERSION_RATE,1) as MATERIAL_COST_USD,
lkp_avg_cost.MATERIAL_OVERHEAD_COST*nvl(lkp_conversion.CONVERSION_RATE,1) as MATARIAL_OVHD_COST_USD,
lkp_avg_cost.LABOUR_COST*nvl(lkp_conversion.CONVERSION_RATE,1) as LABOUR_COST_USD,
lkp_avg_cost.OVERHEAD_COST*nvl(lkp_conversion.CONVERSION_RATE,1) as OVERHEAD_COST_USD,
lkp_avg_cost.OSP_COST*nvl(lkp_conversion.CONVERSION_RATE,1) as OSP_COST_USD,
NULL as FREIGHT_COST_USD,
NULL as UNIT_WEIGHT,
omf.ord_line_shipping_method_code as SHIP_METHOD,
omf.ord_hdr_salesrep_id as SALES_ENGINEER_CODE,
omf.SALESREP_NAME as SALESREP_NAME,
A. NAME as PAYMENT_TERM,
omf.ord_hdr_flow_status_code as ORDER_STATUS_CODE,
omf.ord_line_flow_status_code as LINE_STATUS_CODE,
omf.ord_hdr_trans_type_name as ORDER_TYPE,
omf.trans_type_name as LINE_TYPE,
lkp_categoris.SEGMENT1 as PRODUCT_TIER_1,
lkp_categoris.SEGMENT2 as PRODUCT_TIER_2,
lkp_categoris.SEGMENT3 as PRODUCT_TIER_3,
lkp_categoris.SEGMENT4 as PRODUCT_TIER_4,
lkp_categoris.SEGMENT5 as PRODUCT_TIER_5,
lkp_categoris.SEGMENT6 as PRODUCT_TIER_6,
lkp_categoris.SEGMENT7 as PRODUCT_TIER_7,
lkp_categoris.SEGMENT8 as PRODUCT_TIER_8,
NULL as PRODUCT_TIER_9,
NULL as PRODUCT_TIER_10,
NULL as ATTRIBUTE1,
NULL as ATTRIBUTE2,
NULL as ATTRIBUTE3,
omf.ord_line_attribute4 as ATTRIBUTE4,
NULL as ATTRIBUTE5,
NULL as APPROACH_CODE,
NULL as WORLD_POLE,
lkp_categoris.SEGMENT6 as GO_CLASS_CODE,
NULL as OFFICE_TAKEN_CODE,
NULL as CUSTOMER_GROUP_CODE,
NULL as CUSTOMER_TYPE_DESCRIPTION,
NULL as CESD_USER_NUMBER,
NULL as SPA_BUMBER,
lkp_categoris.SEGMENT3 as MAJOR_LINE_CODE,
NULL as RO_NUMBER,
getMANAGEMENT_ENTITY(lkp_organization.ORGANIZATION_CODE,lkp_gl_code_combination.SEGMENT5,trim(lkp_iqp_legacy_catalog_list_plr.MANAGEMENT_ENTITY),lkp_me_ref.MANAGEMENT_ENTITY) as REPLENISHMENT_SOURCE,
msi.fixed_lead_time as STOCK_SHIP_LEADTIME,
NULL as TRANSACTION_TYPE,
NULL as BAAN_ORDER_NUMBER,
'Y' as ACTUAL_ORDER_LINE_FLAG,
case when msi.ORGANIZATION_ID in (3282,3283,3284) then omf.ord_hdr_attribute13 else '-99' end as SOLD_TO_SITE_NUMBER,
hps1.party_site_number Ship_To_Site_Number,
hps.party_site_number Bill_To_Site_Number,
omf.ord_line_packing_instructions as PACKING_INSTRUCTION,
omf.ord_line_fob_point_code as FOB_POINT_CODE,
1 as DATA_SOURCE,
'Y' as CURRENT_FLAG,
'm_IQP_ORDER_BOOKING_FACT_BRAVO' as RECORD_CREATED_BY,
current_timestamp as RECORD_CREATED_DATE,
'BOOKED' as BOOKING_STATUS,
NULL as INPUT_DATA,
case when upper(br.reporting_process) = 'PULSE REPORTING' then br.rejection_legend else il.inclusion_legend end as report_process1,
case when upper(br.reporting_process) = 'GOVERNMENT REPORTING' then br.rejection_legend else NULL end as report_process2,
case when upper(br.reporting_process) not in ('GOVERNMENT REPORTING','PULSE REPORTING') then br.rejection_legend else NULL end as report_process3,
NULL as REPORT_PROCESS4,
NULL as REPORT_PROCESS5,
NULL as TAG_HISTORY_REPORT_PROCESS,
omf.ORD_HDR_ATTRIBUTE8 as ORD_HDR_ATTRIBUTE8,
omf.ORD_HDR_ATTRIBUTE19 as ORD_HDR_ATTRIBUTE19,
omf.ORD_HDR_ATTRIBUTE1 as ORD_HDR_ATTRIBUTE1,
NULL as SPAN,
NULL as PLANT_DESCRIPTION,
NULL as PLANT_MANAGER_NAME,
getProductPlatform(lkp_business_heir.TIER2,lkp_business_heir.TIER3,lkp_iqp_legacy_catalog_list_plr.ITEM_NUMBER,lkp_iqp_legacy_catalog_list_item.SUB_PRODUCT_LINE,lkp_iqp_legacy_catalog_list_plr.SUB_PRODUCT_LINE,INSTR(omf.PRODUCT_NUMBER,'-'),lkp_iqp_legacy_catalog_list_plr.PRODUCT_PLATFORM,lkp_iqp_legacy_catalog_list_item.PRODUCT_PLATFORM,lkp_organization.ORGANIZATION_CODE) as PRODUCT_PLATFORM,
getProductSegment(lkp_business_heir.TIER2,lkp_iqp_legacy_catalog_list_plr.ITEM_NUMBER,lkp_iqp_legacy_catalog_list_item.PRODUCT_SEGMENT,lkp_iqp_legacy_catalog_list_plr.PRODUCT_SEGMENT,lkp_organization.ORGANIZATION_CODE) as PRODUCT_SEGMENT,
NULL as FAMILY,
(case WHEN omf.ord_hdr_context IN ('Duplicate of ROC_STD_ORDER','ITI_BLANKET_ORDER', 'ITI_EXPORT_ORDER', 'ITI_NPI_PROJECT_ORDER', 'ITI_PSS_ORDER', 'ITI_STD_ORDER', 'MULTILIN_CA_PROJECTS', 'MULTILIN_CA_RMA_LEN', 'MULTILIN_CA_RMA_P&C', 'MULTILIN_CA_RMA_PS', 'MULTILIN_CA_RMA_WARRANTY_LEN', 'MULTILIN_CA_RMA_WARRANTY_P&C', 'MULTILIN_CA_RMA_WARRANTY_PS', 'MULTILIN_CA_STD', 'MULTILIN_CA_UPGRADE', 'MULTILIN_PR_RMA_P&C', 'MULTILIN_PR_RMA_PQ', 'MULTILIN_PR_RMA_PS', 'MULTILIN_PR_RMA_WARRANTY_P&C', 'MULTILIN_PR_RMA_WARRANTY_PQ', 'MULTILIN_PR_RMA_WARRANTY_PS', 'MULTILIN_PR_STD', 'MULTILIN_US_PROJECTS', 'MULTILIN_US_RMA_LEN', 'MULTILIN_US_RMA_P&C', 'MULTILIN_US_RMA_PS', 'MULTILIN_US_RMA_WARRANTY_LEN', 'MULTILIN_US_RMA_WARRANTY_P&C', 'MULTILIN_US_RMA_WARRANTY_PS', 'MULTILIN_US_STD', 'MULTILIN_US_UPGRADE', 'NRPS_ITI_BLANKET_ORDER', 'NRPS_ITI_EXPORT_ORDER', 'NRPS_ITI_PSS_ORDER', 'NRPS_ITI_STD_ORDER', 'ROC_CONCESSION_ORDER', 'ROC_CUST_DEMO_ORDER', 'ROC_INT_DEMO_ORDER', 'ROC_MDS7_CUST_PROJECT_ORDER', 'ROC_MDSA_CUST_PROJECT_ORDER', 'ROC_RMA_ORDER', 'ROC_RMA_REFURB', 'ROC_RMA_WARRANTY_ORDER', 'ROC_STD_ORDER', 'SAS_ITI_BLANKET_ORDER', 'SAS_ITI_PSS_ORDER', 'SAS_ITI_STD_ORDER') THEN   nvl(omf.ord_hdr_attribute1,omf.ord_line_attribute1) WHEN omf.ord_hdr_context IN ('AU_XD_BILL_ONLY_CREDIT_ONLY_SO', 'AU_XD_BILL_ONLY_ORDER', 'AU_XD_DEMO', 'AU_XD_STD_SALES_ORDER', 'MULTILIN_XD_CA_BILL_CREDIT_OLY', 'MULTILIN_XD_CA_DEMO', 'MULTILIN_XD_CA_RMA', 'MULTILIN_XD_CA_RMA_WARRANTY', 'MULTILIN_XD_CA_STD_ORDERS', 'NRPS_XD_BILL_ONLY_ORDER', 'NRPS_XD_DEMO', 'NRPS_XD_RMA_ORDER', 'NRPS_XD_RMA_WARRANTY', 'NRPS_XD_STD_SALES_ORDER', 'NS_LIV_XD_BILL_CREDIT_ONLY', 'NS_LIV_XD_DEMO', 'NS_LIV_XD_RMA', 'NS_LIV_XD_RMA_WARRANTY', 'NS_LIV_XD_STD', 'NS_XD_BILL_ONLY_ORDER_US', 'NS_XD_DEMO_US', 'NS_XD_RMA_ORDER_US', 'NS_XD_RMA_WARRANTY_US', 'NS_XD_STD_ORDER_US', 'XD_RMA_ORDER', 'XD_RMA_WARRANTY') then nvl(omf.ord_hdr_attribute20,omf.ord_line_attribute20) WHEN omf.ord_hdr_context IN ('AU_SMART_SERVS_ORDER', 'AU_SMART_SERVS_ORDER_US', 'BGM_SMART_SERVS_ORDER', 'BGM_SMART_SERVS_ORDER_US', 'CALGARY_COQ_CONCESSION', 'CALGARY_FIELD_REPLACEMENT', 'CALGARY_GROWTH', 'CALGARY_WARRANTY', 'Calgary SmartGrid Product J74S', 'Calgary SmartGrid Return J74S', 'DE_RAT_SMART_SERVS_ORDER', 'DE_RAT_SMART_SERVS_ORDER_US', 'ECS_SALES_ORDER_CA', 'GEPM_QUOTE_ORDER', 'GEPM_SALES_ORDER', 'IED_BDH_SALES_ORDER_DE', 'IED_CAL_NO_CHARGE_ORDER', 'IED_CAL_PREPAYMENT_ORDER', 'IED_CAL_RETURN_ORDER_CA', 'IED_CAL_SALES_ORDER_CA', 'IED_CAL_WARRANTY_RETURN_ORDER', 'IED_CAl_QUOTE_ORDER_CA', 'IED_TAB_SALES_ORDER', 'Kelman Return Sales Order', 'Kelman US Product Sales Order', 'MAPFRAME_LICENSE_ORDER', 'MAPFRAME_LICENSE_RETURN', 'MAPFRAME_RETURN_ORDER', 'MAPFRAME_SALES_ORDER', 'MAR_CST_TR_QUOTE_ORDER', 'MAR_CST_TR_SALES_ORDER', 'MAR_INT_TR_QUOTE_ORDER', 'MAR_INT_TR_SALES_ORDER', 'MAR_PS_ENGSOL_QUOTE_ORDER', 'MAR_PS_ENGSOL_SALES_ORDER', 'MAR_PS_PREPAN_QUOTE_ORDER', 'MAR_PS_PREPAN_SALES_ORDER', 'MAR_PS_PRJSOL_QUOTE_ORDER', 'MAR_PS_PRJSOL_SALES_ORDER', 'MAR_PS_RETSOL_QUOTE_ORDER', 'MAR_PS_RETSOL_SALES_ORDER', 'MAR_STD_TR_QUOTE_ORDER', 'MAR_STD_TR_SALES_ORDER', 'METERS_PRODUCT_SALES_ORDER', 'MON Product Replacement', 'MON Return Product Replacement', 'NL_SMART_SERVS_ORDER', 'NL_SMART_SERVS_ORDER_US', 'NS_CAM_MAINT_ORDER_UK', 'NS_CAM_PROJECT_ORDER_UK', 'NS_CAM_SALES_ORDER_UK', 'NS_DEN_LIC_RETURN_ORDER_US', 'NS_DEN_MAINT_ORDER_US', 'NS_DEN_MAINT_RETURN_ORDER_US', 'NS_DEN_QUOTE_ORDER_US', 'NS_DEN_SALES_ORDER_US', 'NS_DEN_SMART_SERVS_ORDER_US', 'NS_DEN_TRAINING_ORDER_US', 'NS_HW_INSTALLATION_COMMISSION', 'NS_HW_TURNKEY_SERVICES', 'NS_LICENSE_PROJECTS_ORDER_US', 'NS_LIV_PROJECT_ORDER_UK', 'NS_LIV_QUOTE_ORDER_UK', 'NS_LIV_RETURN_ORDER_UK', 'NS_LIV_SALES_ORDER_UK', 'NS_MAP_SMART_SERVS_ORDER_US', 'NS_MEL_QUOTE_ORDER_US', 'NZ_SMART_SERVS_ORDER_US', 'PRJ_CAL_PROJECT_ORDER_CA', 'PRJ_TAB_PROJECT_ORDER', 'Return SmartGrid Mapframe', 'SAS_BOI_QUOTE_ORDER_US', 'SAS_BOI_SALES_ORDER_US', 'SAS_CHI_PROJECT_ORDER_US', 'SAS_CHI_QUOTE_ORDER_US', 'SAS_CHI_RETURN_ORDER_US', 'SAS_CHI_SALES_ORDER_US', 'SAS_LED_QUOTE_ORDER_UK', 'SAS_LED_RETURN_ORDER_UK', 'SAS_MON_PROJECT_ORDER_CA', 'SAS_MON_RETURN_ORDER_CA', 'SAS_MON_SALES_ORDER_CA', 'SAS_MON_WARRANTY_RETURN_ORDER', 'SAS_PRO_QUOTE_ORDER_US', 'SGD_SMART_SERVS_ORDER', 'SGD_SMART_SERVS_ORDER_US', 'SP_SMART_SERVS_ORDER', 'SP_SMART_SERVS_ORDER_US', 'SW_AU_SALES_ORDER_AU', 'SW_NZ_QUOTE_ORDER_NZ', 'SW_NZ_SALES_ORDER_NZ', 'SW_RAT_MAINT_ORDER_DE', 'SW_RAT_MAINT_RETURN_ORDER_DE', 'SW_RAT_QUOTE_ORDER_DE', 'SW_RAT_RETURN_ORDER_DE', 'SW_RAT_SALES_ORDER_DE', 'SW_RAT_SERVICE_ORDER_DE', 'SmallWorld DE Licenses Return', 'SmallWrld AU License Return', 'SmallWrld AU Licenses', 'SmallWrld Belgium Licen Return', 'SmallWrld Belgium Licenses', 'SmallWrld DE Licenses', 'SmallWrld NL Licenses', 'SmallWrld NL Licenses Return', 'SmallWrld NZ License Return', 'SmallWrld NZ Licenses', 'SmallWrld SGD Licenses', 'SmallWrld SP Licenses', 'SmallWrld SP Licenses Return', 'SmallWrld UK Licenses', 'SmallWrld UK Licenses Return', 'Smart Grid Denver Return SGSL', 'Smart Grid Denver SGSL', 'SmartGrid Kelman - DG1S', 'SmartGrid Kelman Return- DG1S', 'SmartGrid Mapframe MAP3', 'SmartGrid Montreal Return 575S', 'SmartGrid Montreal Sales 575S', 'Swisscom Ops Return Order SWOP', 'Swisscom Ops Sales Order -SWOP', 'TD_CAL_COQ Concession', 'TD_MAINT_ORDER_BGM', 'TD_MAINT_ORDER_NL', 'TD_MAINT_ORDER_SGD', 'TD_MAINT_ORDER_SP', 'TD_RETURN_ORDER_BGM', 'TD_RETURN_ORDER_NL', 'TD_RETURN_ORDER_SGD', 'TD_RETURN_ORDER_SP', 'TD_SALES_ORDER_BGM', 'TD_SALES_ORDER_NL', 'TD_SALES_ORDER_SGD', 'TD_SALES_ORDER_SP', 'TD_TRAINING_ORDER_BGM', 'TD_TRAINING_ORDER_NL', 'TD_TRAINING_ORDER_SGD', 'TD_TRAINING_ORDER_SP', 'UK_CAM_SMART_SERVS_ORDER', 'UK_CAM_SMART_SERVS_ORDER_US') then nvl(omf.ord_hdr_attribute3,omf.ord_line_attribute3) when omf.ord_line_context IN ('2113') THEN   nvl(omf.ord_hdr_attribute12,omf.ord_line_attribute12) ELSE   nvl(omf.ord_hdr_attribute1,omf.ord_line_attribute1) END )   end_destination_country,
(case
WHEN omf.ord_hdr_context IN ('Duplicate of ROC_STD_ORDER','ITI_BLANKET_ORDER','ITI_EXPORT_ORDER','ITI_NPI_PROJECT_ORDER','ITI_PSS_ORDER','ITI_STD_ORDER','NRPS_ITI_BLANKET_ORDER','NRPS_ITI_EXPORT_ORDER','NRPS_ITI_PSS_ORDER','NRPS_ITI_STD_ORDER','ROC_CONCESSION_ORDER','ROC_CUST_DEMO_ORDER','ROC_MDS7_CUST_PROJECT_ORDER','ROC_MDSA_CUST_PROJECT_ORDER','ROC_RMA_ORDER','ROC_RMA_REFURB','ROC_RMA_WARRANTY_ORDER','ROC_STD_ORDER','SAS_ITI_BLANKET_ORDER','SAS_ITI_PSS_ORDER','SAS_ITI_STD_ORDER') THEN
nvl(omf.ord_hdr_attribute2,omf.ord_line_attribute2)
WHEN omf.ord_hdr_context IN ('MULTILIN_CA_PROJECTS','MULTILIN_CA_RMA_LEN','MULTILIN_CA_RMA_P&C','MULTILIN_CA_RMA_PS','MULTILIN_CA_RMA_WARRANTY_LEN','MULTILIN_CA_RMA_WARRANTY_P&C','MULTILIN_CA_RMA_WARRANTY_PS','MULTILIN_CA_STD','MULTILIN_CA_UPGRADE','MULTILIN_PR_RMA_P&C','MULTILIN_PR_RMA_PQ','MULTILIN_PR_RMA_PS','MULTILIN_PR_RMA_WARRANTY_P&C','MULTILIN_PR_RMA_WARRANTY_PQ','MULTILIN_PR_RMA_WARRANTY_PS','MULTILIN_PR_STD','MULTILIN_US_PROJECTS','MULTILIN_US_RMA_LEN','MULTILIN_US_RMA_P&C','MULTILIN_US_RMA_PS','MULTILIN_US_RMA_WARRANTY_LEN','MULTILIN_US_RMA_WARRANTY_P&C','MULTILIN_US_RMA_WARRANTY_PS','MULTILIN_US_STD','MULTILIN_US_UPGRADE') THEN
nvl(omf.ord_hdr_attribute17,omf.ord_line_attribute17)
ELSE
  nvl(omf.ord_hdr_attribute20,omf.ord_line_attribute20)
END )  end_destination_state,
getProductSegment(lkp_business_heir.TIER2,lkp_iqp_legacy_catalog_list_plr.ITEM_NUMBER,lkp_iqp_legacy_catalog_list_item.PRODUCT_LINE,lkp_iqp_legacy_catalog_list_plr.PRODUCTLINE,lkp_organization.ORGANIZATION_CODE) as PRODUCT_LINE,
case when lkp_business_heir.TIER2='GRID AUTOMATION' AND (lkp_business_heir.TIER3='PROTECTION & CONTROL' OR lkp_business_heir.TIER3='SAS')
then nvl(lkp_iqp_sub_business.SUB_BUSINESS,'PRODUCT ORDERS')
else 'PRODUCT ORDERS' end as ORDER_TYPE_PULSE,
NULL as SOLD_TO_STATE_CODE,
NULL as SOLD_TO_COUNTRY_CODE,
NULL as BILL_TO_COUNTRY_CODE,
NULL as BILL_TO_STATE_CODE,
NULL as SHIP_TO_COUNTRY_CODE,
omf.ITEM_TYPE_CODE as ITEM_TYPE_CODE,
NULL as IMPORTING_TEXTFILE,
NULL as GLO_ULT_DUNS,
NULL as STANDARD_PARENT_CUSTOMERNAME,
NULL as GE_GLOBAL_DUNS,
NULL as GE_DUNS_NAME,
NULL as STANDARD_CUSTOMERNAME,
NULL as STANDARD_CUSTOMERNUMBER,
NULL as SM_NAME,
NULL as SM_OHR,
NULL as SALES_REP,
NULL as COMPONENTFLAG,
getProductSegment(lkp_business_heir.TIER2,trim(lkp_iqp_legacy_catalog_list_plr.BUSINESSSEGMENT)," "," ",lkp_organization.organization_code) as BUSINESS,
getPRODUCTCLASSGROUP(lkp_business_heir.TIER2,lkp_business_heir.TIER3,lkp_iqp_legacy_catalog_list_plr.ITEM_NUMBER,lkp_iqp_legacy_catalog_list_item.PRODUCT_GROUP,INSTR(omf.PRODUCT_NUMBER,'-'),lkp_iqp_legacy_catalog_list_plr.PRODUCT_GROUP,lkp_iqp_legacy_catalog_list_item.PRODUCTCLASS,lkp_iqp_legacy_catalog_list_plr.PRODUCTCLASS,lkp_business_heir.PNL,SUBSTR(omf.Operational_Department,1,3)) as PRODUCTCLASSGROUP,
NULL AS SUBSECTION,
NULL AS WORLD_SUB_POLE,
NULL AS STATE_CODE,
NULL AS SHIP_DATE,
NULL AS SHIPSTATECODE,
NULL AS ERROR_CODE,
NULL AS FOREIGN_CURRENCY_CODE,
NULL AS MANUFACTURING_DESCRIPTION,
NULL AS PRODUCT_PLATFORM_RMA,
NULL AS PRODUCT_GROUP_RMA,
NULL AS PRODUCT_LINE_RMA,
NULL AS PRODUCT_CLASS_RMA,
NULL AS SHIPTO_SITENUMBER,
NULL AS CUSTOMER_NUMBER,
lkp_ship_to_customer.SHIP_TO_REGION as SHIP_TO_REGION,
lkp_bill_to_customer.BILL_TO_REGION as BILL_TO_REGION,
lkp_ship_to_customer.CUSTOMER_GROUP as SHIP_TO_CUST_GROUP,
lkp_ship_to_customer.CUSTOMER_CLASSIFICATION as CUSTOMER_CLASSIFICATION,
omf.INVOICE_TO_CUST_NUMBER as BILL_TO_CUST_GROUP,
omf.INVOICE_TO_CUST_NAME as BILL_TO_CLASSIFY,
NULL AS CANCELLED_FLAG,
NULL AS FOREIGN_CURRECY_RATE,
NULL AS REVISED_CHANNEL,
omf.LINE_CATEGORY_CODE,
lkp_ship_to_customer.SHIP_TO_SUB_REGION,
lkp_bill_to_customer.BILL_TO_SUB_REGION,
NULL AS SITE,
NULL AS PRODUCTDESC_BUSRPT,
NULL AS PROJECT_NUMBER,
NULL AS FAMILY_BUSRPT,
NULL AS SUBFAMILY_BUSRPT,
LKP_BUSINESS_HEIR.PNL as PNL,
lkp_orig_doc_ref.ORD_HDR_ORIG_SYS_DOCUMENT_REF as ORIG_SYS_DOCUMENT_REF,
NULL AS DISTRICT_CODE,
NULL AS ATM_CODE,
NULL AS ATTRIBUTE12,
NULL AS ATTRIBUTE13,
omf.OPTION_NUMBER,
omf.SHIPMENT_NUMBER,
HZP1.PARTY_NAME AS END_USER,
LOC1.COUNTRY AS END_USER_COUNTRY,
oola.ATTRIBUTE13,
omf.ord_line_attribute12 as END_USER_COUNTRY1,
omf.SOURCE_TYPE_CODE as SOURCE_TYPE_CODE,
omf.vendor_name as vendor_name,
omf.ord_hdr_attribute16 as End_User_City,
omf.ord_hdr_attribute17 as END_USER_STATE,
NULL AS ADJUSTMENT_TYPE,
NULL AS DELETE_FLAG,
omf.PRODUCT_SUB_LINE,
omf.EARLIEST_ACCEPTABLE_DATE as EARLIEST_ACCEPTABLE_DATE,
NULL AS REASON_FOR_MANUAL_ADJUSTMENT,
case when lkp_business_heir.TIER2='GRID AUTOMATION'
then nvl(lkp_iqp_sub_business1.BUSINESS_SEGMENT,'TBD') end as BUSINESS_SEGMENT,
case when (getMANAGEMENT_ENTITY(lkp_organization.ORGANIZATION_CODE,lkp_gl_code_combination.SEGMENT5,trim(lkp_iqp_legacy_catalog_list_plr.MANAGEMENT_ENTITY),lkp_me_ref.MANAGEMENT_ENTITY)) in ('DG24', 'DG14', '5750') then (case when omf.ITEM_TYPE_CODE='CONFIG' AND lkp_organization.ORGANIZATION_CODE = 'C71' then lkp_config_unit_price.UNIT_SELLING_PRICE*((case when omf.line_category_code ='RETURN' then
nvl(omf.ordered_quantity, 0) * (-1) else
nvl(omf.ordered_quantity, 0) end))*nvl(lkp_conversion.CONVERSION_RATE,1) else (case when omf.line_category_code ='RETURN' then
nvl(omf.ordered_quantity, 0) * (-1) else
nvl(omf.ordered_quantity, 0) end)*omf.UNIT_SELLING_PRICE*nvl(lkp_conversion.CONVERSION_RATE,1) end)-(1.18*(lkp_avg_cost.MATERIAL_COST*lkp_conversion.CONVERSION_RATE))
else ((case when omf.ITEM_TYPE_CODE='CONFIG' AND lkp_organization.ORGANIZATION_CODE = 'C71' then lkp_config_unit_price.UNIT_SELLING_PRICE*(case when omf.line_category_code ='RETURN' then
nvl(omf.ordered_quantity, 0) * (-1) else
nvl(omf.ordered_quantity, 0) end)*nvl(lkp_conversion.CONVERSION_RATE,1) else (case when omf.line_category_code ='RETURN' then
nvl(omf.ordered_quantity, 0) * (-1) else
nvl(omf.ordered_quantity, 0) end)*omf.UNIT_SELLING_PRICE*nvl(lkp_conversion.CONVERSION_RATE,1) end)-((lkp_avg_cost.MATERIAL_COST*lkp_conversion.CONVERSION_RATE)+lkp_avg_cost.MATERIAL_OVERHEAD_COST+lkp_avg_cost.LABOUR_COST+lkp_avg_cost.OVERHEAD_COST*lkp_conversion.CONVERSION_RATE+lkp_avg_cost.OSP_COST*lkp_conversion.CONVERSION_RATE)) 
end as CM_AMOUNT,
NULL AS BURDENED_COST,
case when upper(case when upper(br.reporting_process) = 'PULSE REPORTING' then br.rejection_legend else il.inclusion_legend end) = 'PULSE REPORTING' then smd.rsm_nm else NULL end AS RSM_NM,
case when upper(case when upper(br.reporting_process) = 'PULSE REPORTING' then br.rejection_legend else il.inclusion_legend end) = 'PULSE REPORTING' then smd.srep_nm else NULL end AS srep_nm,
NULL AS BAAN_PO,
NULL AS EARLIEST_ACCEPTABLE_DATE_SPAIN,
lkp_oracle_po_number.PO_NUM AS ORACLE_PO_NUMBER,
cast(msi.INVENTORY_ITEM_ID AS String) AS INVENTORY_ITEM_ID,
cast(msi.ORGANIZATION_ID as STRING) as ORGANIZATION_ID,
omf.ord_line_invoice_to_org_id as INVOICE_TO_ORG_ID,
omf.ord_line_ship_to_org_id as ship_to_org_id,
gcc.code_combination_id code_combination_id,
omf.ord_hdr_salesrep_id as SALESREP_ID
from 
g00103.iqp_order_mgmt_fact omf inner join g00103.MTL_SYSTEM_ITEMS_B msi on (omf.inventory_item_id = msi.inventory_item_id and 
omf.ord_line_ship_from_org_id = msi.organization_id) inner join g00103.hz_cust_site_uses_all  hzsu1 on 
(omf.ord_line_ship_to_org_id = hzsu1.site_use_id and hzsu1.site_use_code = 'SHIP_TO') inner join g00103.hz_cust_acct_sites_all hzs1 on (hzsu1.cust_acct_site_id=hzs1.cust_acct_site_id)
inner join g00103.hz_cust_accounts hzca1 on (hzca1.cust_account_id = hzs1.cust_account_id) inner join g00103.hz_parties hzp1 on 
(hzp1.party_id = hzca1.party_id) 
inner join g00103.hz_cust_site_uses_all  hzsu on (omf.ord_line_invoice_to_org_id=hzsu.site_use_id and hzsu.site_use_code = 'BILL_TO')
inner join g00103.hz_cust_acct_sites_all hzs on (hzs.cust_acct_site_id = hzsu.cust_acct_site_id) 
inner join g00103.hz_party_sites hps1 on (hzs1.party_site_id = hps1.party_site_id) 
inner join g00103.hz_locations loc1 on (hps1.location_id = loc1.location_id) 
inner join g00103.hz_cust_accounts hzca on (hzca.cust_account_id = hzs.cust_account_id)
inner join g00103.hz_parties hzp on (hzp.party_id = hzca.party_id) 
inner join g00103.hz_party_sites hps on (hzs.party_site_id = hps.party_site_id)
inner join g00103.hz_locations loc on (hps.location_id = loc.location_id)
inner join g00103.OE_TRANSACTION_TYPES_ALL ott on (omf.order_type_id = ott.transaction_type_id)
inner join g00103.OE_TRANSACTION_TYPES_ALL ottl on (omf.line_type_id = ottl.transaction_type_id)
inner join g00103.gl_code_combinations gcc on ((case when omf.ord_line_ship_from_org_id in ('2453','3954','3955','3956','4074') then OTTL.COST_OF_GOODS_SOLD_ACCOUNT
else ott.cost_of_goods_sold_account end) = gcc.code_combination_id)
inner join df_oe_order_lines_all oola on (oola.header_id = omf.header_id and oola.line_id = omf.line_id)
left join lkp_bill_to_customer on (omf.ord_line_invoice_to_org_id=lkp_bill_to_customer.CUST_SITE_USES_SITE_USE_ID)
left join lkp_gl_code_combination on (gcc.code_combination_id=lkp_gl_code_combination.CODE_COMBINATION_ID)
left join lkp_iqp_legacy_catalog_list_plr on (msi.inventory_item_id=lkp_iqp_legacy_catalog_list_plr.INVENTORY_ITEM_ID and
msi.organization_id=lkp_iqp_legacy_catalog_list_plr.PLANTCODE)
left join lkp_ship_to_customer on (omf.ord_line_ship_to_org_id=lkp_ship_to_customer.CUST_SITE_USES_SITE_USE_ID)
left join lkp_geography_country_state on ( loc1.state = lkp_geography_country_state.STATE_CODE and
loc1.country = lkp_geography_country_state.COUNTRY_CODE)
left join lkp_state on (lkp_ship_to_customer.LOC_STATE = lkp_state.STATE_CODE)
left join lkp_geography_country on (loc1.country = lkp_geography_country.COUNTRY_CODE)
left join lkp_organization on (msi.organization_id=lkp_organization.ORGANIZATION_ID)
left join lkp_iqp_legacy_catalog_list_item on (msi.segment1 = lkp_iqp_legacy_catalog_list_item.ITEM_NUMBER)
left join lkp_me_ref on (lkp_organization.ORGANIZATION_CODE=lkp_me_ref.ORG_CODE)
left join lkp_business_heir on (getMANAGEMENT_ENTITY(lkp_organization.ORGANIZATION_CODE,lkp_gl_code_combination.SEGMENT5,trim(lkp_iqp_legacy_catalog_list_plr.MANAGEMENT_ENTITY),lkp_me_ref.MANAGEMENT_ENTITY) = lkp_business_heir.PLANTCODE_ID)
left join lkp_sold_to_customer11 on (case when msi.organization_id in(3282,3283,3284) then omf.ORD_HDR_ATTRIBUTE13 else '-99' end = lkp_sold_to_customer11.PARTY_SITE_NUMBER)
left join lkp_avg_cost on ( msi.inventory_item_id=lkp_avg_cost.inventory_item_id and
msi.ORGANIZATION_ID=lkp_avg_cost.ORGANIZATION_ID)
left join lkp_conversion on (to_date(NVL(to_date(omf.booked_date), to_date(omf.ordered_date)))= to_date(lkp_conversion.CONVERSION_DATE) and
cast(omf.transactional_curr_code as String)= cast(lkp_conversion.FROM_CURRENCY as String))
left join (SELECT DISTINCT NAME,TERM_ID   FROM g00103.RA_TERMS_VL ) A on (A.TERM_ID= omf.ord_line_payment_term_id)
left join lkp_categoris on (msi.INVENTORY_ITEM_ID=lkp_categoris.INVENTORY_ITEM_ID and
msi.organization_id=lkp_categoris.WAREHOUSE_ID)
left join lkp_iqp_sub_business on (getProductPlatform(lkp_business_heir.TIER2,lkp_business_heir.TIER3,lkp_iqp_legacy_catalog_list_plr.ITEM_NUMBER,lkp_iqp_legacy_catalog_list_item.SUB_PRODUCT_LINE,lkp_iqp_legacy_catalog_list_plr.SUB_PRODUCT_LINE,INSTR(omf.PRODUCT_NUMBER,'-'),lkp_iqp_legacy_catalog_list_plr.PRODUCT_PLATFORM,lkp_iqp_legacy_catalog_list_item.PRODUCT_PLATFORM,lkp_organization.ORGANIZATION_CODE)= lkp_iqp_sub_business.PRODUCT_PLATFORM and lkp_business_heir.TIER3 = lkp_iqp_sub_business.PNL)
left join lkp_orig_doc_ref on (omf.ORDER_NUMBER=lkp_orig_doc_ref.ORDER_NUMBER)
left join lkp_iqp_sub_business1 on (getProductPlatform(lkp_business_heir.TIER2,lkp_business_heir.TIER3,lkp_iqp_legacy_catalog_list_plr.ITEM_NUMBER,lkp_iqp_legacy_catalog_list_item.SUB_PRODUCT_LINE,lkp_iqp_legacy_catalog_list_plr.SUB_PRODUCT_LINE,INSTR(omf.PRODUCT_NUMBER,'-'),lkp_iqp_legacy_catalog_list_plr.PRODUCT_PLATFORM,lkp_iqp_legacy_catalog_list_item.PRODUCT_PLATFORM,lkp_organization.ORGANIZATION_CODE)= lkp_iqp_sub_business1.PRODUCT_PLATFORM and
lkp_business_heir.TIER3 = lkp_iqp_sub_business1.PNL) left join
lkp_oracle_po_number on (omf.LINE_ID = lkp_oracle_po_number.LINE_ID)
left join lkp_iqp_oe_order_lines_shipment_no on (omf.LINE_ID = lkp_iqp_oe_order_lines_shipment_no.LINE_ID and 
omf.HEADER_ID = lkp_iqp_oe_order_lines_shipment_no.HEADER_ID)
left join lkp_config_unit_price on (omf.HEADER_ID = lkp_config_unit_price.HEADER_ID and
omf.LINE_NUMBER = lkp_config_unit_price.LINE_NUMBER and
lkp_iqp_oe_order_lines_shipment_no.SHIPMENT_NUMBER = lkp_config_unit_price.SHIPMENT_NUMBER)
left join lkp_business_heir J on (cast(getMANAGEMENT_ENTITY(lkp_organization.ORGANIZATION_CODE,lkp_gl_code_combination.SEGMENT5,trim(lkp_iqp_legacy_catalog_list_plr.MANAGEMENT_ENTITY),lkp_me_ref.MANAGEMENT_ENTITY) as String) = cast(J.PLANTCODE_ID as String) and ((
		lkp_organization.ORGANIZATION_CODE IN ('C71')
		AND omf.ITEM_TYPE_CODE NOT          IN ('STANDARD', 'CONFIG')
		)
	OR 
		(
		UPPER(J.TIER3) NOT            IN  ('METERS', 'ACLARA METERS')
		AND omf.ord_line_flow_status_code           IN ('ENTERED')
		)

	OR 
		(UPPER(J.TIER3) NOT            IN  ('METERS', 'ACLARA METERS')
		AND omf.ord_hdr_flow_status_code           IN ('ENTERED')
		)
	OR omf.ord_hdr_flow_status_code            IN ('CANCELLED','PENDING CUSTOMER ACCEPTANCE','OFFER EXPIRED','DRAFT')
	OR UPPER(getMANAGEMENT_ENTITY(lkp_organization.ORGANIZATION_CODE,lkp_gl_code_combination.SEGMENT5,trim(lkp_iqp_legacy_catalog_list_plr.MANAGEMENT_ENTITY),lkp_me_ref.MANAGEMENT_ENTITY))     IN ('19RB', '19RU', '19RC', 'J74', 'J7P')
	OR 
		( omf.trans_type_name LIKE '%RETURN%'
		OR omf.trans_type_name LIKE'%NO SALES CREDIT%'
		OR omf.trans_type_name LIKE'%QUOTE%'
		OR omf.trans_type_name LIKE'%CANCELLED%' )
	OR 
		( omf.ord_hdr_trans_type_name LIKE '%RETURN%'
		OR omf.ord_hdr_trans_type_name LIKE'%QUOTE%'
		OR omf.ord_hdr_trans_type_name LIKE'%NO CREDIT%'
		OR omf.ord_hdr_trans_type_name LIKE'%RMA%WARRANTY%' )
	OR omf.ord_line_flow_status_code              IN ('CANCELLED','PENDING CUSTOMER ACCEPTANCE','OFFER EXPIRED','DRAFT')
	OR 
		(lkp_organization.ORGANIZATION_CODE NOT IN ('C55','C57','C58')
		and UPPER(omf.LINE_CATEGORY_CODE)    in ('RETURN'))
		))
left join (select BUSINESS_RULE_ID,REPORTING_PROCESS,REJECTION_LEGEND from g00103.IQP_BUSINESS_RULE_DIM) br on (13 = br.BUSINESS_RULE_ID and ((
		lkp_organization.ORGANIZATION_CODE IN ('C71')
		AND omf.ITEM_TYPE_CODE NOT          IN ('STANDARD', 'CONFIG')
		)
	OR 
		(
		UPPER(J.TIER3) NOT            IN  ('METERS', 'ACLARA METERS')
		AND omf.ord_line_flow_status_code            IN ('ENTERED')
		)

	OR 
		(UPPER(J.TIER3) NOT            IN  ('METERS', 'ACLARA METERS')
		AND omf.ord_hdr_flow_status_code           IN ('ENTERED')
		)
	OR omf.ord_hdr_flow_status_code            IN ('CANCELLED','PENDING CUSTOMER ACCEPTANCE','OFFER EXPIRED','DRAFT')
	OR UPPER(getMANAGEMENT_ENTITY(lkp_organization.ORGANIZATION_CODE,lkp_gl_code_combination.SEGMENT5,trim(lkp_iqp_legacy_catalog_list_plr.MANAGEMENT_ENTITY),lkp_me_ref.MANAGEMENT_ENTITY))     IN ('19RB', '19RU', '19RC', 'J74', 'J7P')
	OR 
		( omf.trans_type_name LIKE '%RETURN%'
		OR omf.trans_type_name LIKE'%NO SALES CREDIT%'
		OR omf.trans_type_name LIKE'%QUOTE%'
		OR omf.trans_type_name LIKE'%CANCELLED%' )
	OR 
		( omf.ord_hdr_trans_type_name LIKE '%RETURN%'
		OR omf.ord_hdr_trans_type_name LIKE'%QUOTE%'
		OR omf.ord_hdr_trans_type_name LIKE'%NO CREDIT%'
		OR omf.ord_hdr_trans_type_name LIKE'%RMA%WARRANTY%' )
	OR omf.ord_line_flow_status_code              IN ('CANCELLED','PENDING CUSTOMER ACCEPTANCE','OFFER EXPIRED','DRAFT')
	OR 
		(lkp_organization.ORGANIZATION_CODE NOT IN ('C55','C57','C58')
		and UPPER(omf.LINE_CATEGORY_CODE)    in ('RETURN') )
		))left join (select order_header_id,order_line_id,max(rsm_nm) as rsm_nm,max(srep_nm) as srep_nm,max(order_number) as order_number from g00103.iqp_smd_us_ref group by order_header_id,order_line_id) smd on (omf.header_id = smd.order_header_id and omf.line_id = smd.order_line_id)
		cross join (select distinct inclusion_legend from g00103.IQP_BUSINESS_RULE_DIM where table_name = 'IQP_ORDER_BOOKING_FACT') il
where 
omf.transaction_phase_code <> 'N' and 
omf.record_created_by = 'SPARK_IQP_ORDER_MANAGEMENT_FACT'
""")
final_query.createOrReplaceTempView("df_final_w_tag_query")

//for loading the temp table
val temp_table_load = spark.sql("""select df_final_w_tag_query.*,lkp_target.*
                                                                        from df_final_w_tag_query left join
                                                                        lkp_target on (df_final_w_tag_query.order_line_id=lkp_target.neg_order_line_id and df_final_w_tag_query.order_header_id=lkp_target.neg_order_header_id)""")
temp_table_load.createOrReplaceTempView("df_temp_table_load")
spark.sql("cache table df_temp_table_load")
spark.sql("truncate table g00103.booking_fact_temp")
val sql4 = s"""Insert into table g00103.booking_fact_temp select * from df_temp_table_load"""
spark.sql(sql4)

//for new record
val new_record = spark.sql("""select order_header_id,
order_line_id, order_number, line_number, catlog_number, catlog_description, customer_po_nbr, ship_to_customer_number, ship_to_customer_name, customer_group, ship_to_address_1, ship_to_address_2, ship_to_address_3, ship_to_address_4, ship_to_city, ship_to_state, ship_to_country, ship_to_zip_code, bill_to_customer_number, bill_to_customer_name, bill_to_address_1, bill_to_address_2, bill_to_address_3, bill_to_address_4, bill_to_city, bill_to_state, bill_to_country, bill_to_zip_code, sold_to_customer_number, sold_to_customer_name, sold_to_address_1, sold_to_address_2, sold_to_address_3, sold_to_address_4, sold_to_city, sold_to_state, sold_to_country, sold_to_zip_code, end_user_name, customer_request_date, order_receipt_date, ordered_date, order_entry_date, booked_date, promise_date, scheduled_ship_date, shipped_date, process_date, quote_date, product_class_code, sales_channel, ship_from_warehouse_name, ship_from_warehouse_code, management_entity, region, subregion, currency_code, net_quantity, unit_selling_price, net_amount_func_currency, material_cost, matarial_ovhd_cost, labor_cost, overhead_cost, osp_cost, commissions, freight, usd_conversion_rate, net_amount_usd, material_cost_usd, matarial_ovhd_cost_usd, labour_cost_usd, overhead_cost_usd, osp_cost_usd, freight_cost_usd, unit_weight, ship_method, sales_engineer_code, sales_engineer_name, payment_term, order_status_code, line_status_code, order_type, line_type, product_tier_1, product_tier_2, product_tier_3, product_tier_4, product_tier_5, product_tier_6, product_tier_7, product_tier_8, product_tier_9, product_tier_10, attribute1, attribute2, attribute3, attribute4, attribute5, approach_code, world_pole, go_class_code, office_taken_code, customer_group_code, customer_type_description, cesd_user_number, spa_bumber, major_line_code, ro_number, replenishment_source, stock_ship_leadtime, transaction_type, baan_order_number, actual_order_line_flag, sold_to_site_number, ship_to_site_number, bill_to_site_number, packing_instruction, fob_point_code,  current_flag, record_created_by, record_created_date, booking_status, input_data, report_process1, report_process2, report_process3, report_process4, report_process5, tag_history_report_process, ord_hdr_attribute8, ord_hdr_attribute19, ord_hdr_attribute1, span, plant_description, plant_manager_name, product_platform, product_segment, family, end_destination_country, end_destination_state, product_line, order_type_pulse, sold_to_state_code, sold_to_country_code, bill_to_country_code, bill_to_state_code, ship_to_country_code, item_type_code, importing_textfile, glo_ult_duns, standard_parent_customername, ge_global_duns, ge_duns_name, standard_customername, standard_customernumber, sm_name, sm_ohr, sales_rep, componentflag, business, productclassgroup, subsection, world_sub_pole, state_code, ship_date, shipstatecode, error_code, foreign_currency_code, manufacturing_description, product_platform_rma, product_group_rma, product_line_rma, product_class_rma, shipto_sitenumber, customer_number, ship_to_region, bill_to_region, ship_to_cust_group, ship_to_classify, bill_to_cust_group, bill_to_classify, cancelled_flag, foreign_currecy_rate, revised_channel, line_category_code, ship_to_sub_region, bill_to_sub_region, site, productdesc_busrpt, project_number, family_busrpt, subfamily_busrpt, pnl, orig_sys_document_ref, district_code, atm_code, attribute12, attribute13, option_number, shipment_number, end_user, end_user_country, end_user_region, end_user_country1, source_type_code, vendor_name, end_user_city, end_user_state, adjustment_type, delete_flag, product_sub_line, earliest_acceptable_date, reason_for_manual_adjustment, business_segment, cm_amount, burdened_cost, rsm_nm, srep_nm, baan_po, earliest_acceptable_date_spain, oracle_po_number, inventory_item_id, organization_id, invoice_to_org_id, ship_to_org_id, code_combination_id, salesrep_id, 1,data_source from g00103.booking_fact_temp 
where NEG_ORDER_LINE_ID is null and NEG_ORDER_HEADER_ID is null and order_status_code <> 'CANCELLED' and line_status_code <> 'CANCELLED' """)


val existing_record_c = spark.sql("""select NEG_ORDER_HEADER_ID,
NEG_ORDER_LINE_ID, order_number, line_number, catlog_number, catlog_description, customer_po_nbr, ship_to_customer_number, ship_to_customer_name, NEG_CUSTOMER_GROUP, ship_to_address_1, ship_to_address_2, ship_to_address_3, ship_to_address_4, ship_to_city, ship_to_state, ship_to_country, ship_to_zip_code, bill_to_customer_number, bill_to_customer_name, bill_to_address_1, bill_to_address_2, bill_to_address_3, bill_to_address_4, bill_to_city, bill_to_state, bill_to_country, bill_to_zip_code, sold_to_customer_number, sold_to_customer_name, sold_to_address_1, sold_to_address_2, sold_to_address_3, sold_to_address_4, sold_to_city, sold_to_state, sold_to_country, sold_to_zip_code, end_user_name, customer_request_date, order_receipt_date, ordered_date, order_entry_date, booked_date, promise_date, scheduled_ship_date, shipped_date, process_date, quote_date, product_class_code, sales_channel, ship_from_warehouse_name, NEG_SHIP_FROM_WAREHOUSE_CODE, NEG_MANAGEMENT_ENTITY, region, subregion, currency_code, -1*NEG_NET_QUANTITY, -1*NEG_UNIT_SELLING_PRICE, -1*NEG_NET_AMOUNT_FUNC_CURRENCY, -1*NEG_MATERIAL_COST, -1*NEG_MATARIAL_OVHD_COST, -1*NEG_LABOR_COST, -1*NEG_OVERHEAD_COST, -1*NEG_OSP_COST, -1*NEG_COMMISSIONS, -1*NEG_FREIGHT, usd_conversion_rate, -1*NEG_NET_AMOUNT_USD, -1*NEG_MATERIAL_COST_USD, -1*NEG_MATARIAL_OVHD_COST_USD, -1*NEG_LABOUR_COST_USD, -1*NEG_OVERHEAD_COST_USD, -1*NEG_OSP_COST_USD, NEG_FREIGHT_COST_USD, unit_weight, ship_method, sales_engineer_code, sales_engineer_name, payment_term, NEG_ORDER_STATUS_CODE, NEG_LINE_STATUS_CODE, order_type, line_type, product_tier_1, product_tier_2, product_tier_3, product_tier_4, product_tier_5, product_tier_6, product_tier_7, product_tier_8, product_tier_9, product_tier_10, attribute1, attribute2, attribute3, attribute4, attribute5, approach_code, world_pole, go_class_code, office_taken_code, customer_group_code, customer_type_description, cesd_user_number, spa_bumber, major_line_code, ro_number, replenishment_source, stock_ship_leadtime, transaction_type, baan_order_number, actual_order_line_flag, sold_to_site_number, ship_to_site_number, bill_to_site_number, packing_instruction, fob_point_code,  current_flag, record_created_by, record_created_date, booking_status, input_data, report_process1, report_process2, report_process3, report_process4, report_process5, tag_history_report_process, ord_hdr_attribute8, ord_hdr_attribute19, ord_hdr_attribute1, span, plant_description, plant_manager_name, product_platform, product_segment, family, end_destination_country, end_destination_state, product_line, order_type_pulse, sold_to_state_code, sold_to_country_code, bill_to_country_code, bill_to_state_code, ship_to_country_code, item_type_code, importing_textfile, glo_ult_duns, standard_parent_customername, ge_global_duns, ge_duns_name, standard_customername, standard_customernumber, sm_name, sm_ohr, sales_rep, componentflag, business, productclassgroup, subsection, world_sub_pole, state_code, ship_date, shipstatecode, error_code, foreign_currency_code, manufacturing_description, product_platform_rma, product_group_rma, product_line_rma, product_class_rma, shipto_sitenumber, customer_number, ship_to_region, bill_to_region, ship_to_cust_group, ship_to_classify, bill_to_cust_group, bill_to_classify, case when ORDER_STATUS_CODE = 'CANCELLED' or LINE_STATUS_CODE = 'CANCELLED' then 'Y' else 'N' end as cancelled_flag, foreign_currecy_rate, revised_channel, line_category_code, ship_to_sub_region, bill_to_sub_region, site, productdesc_busrpt, project_number, family_busrpt, subfamily_busrpt, pnl, orig_sys_document_ref, district_code, atm_code, attribute12, attribute13, option_number, shipment_number, end_user, end_user_country, end_user_region, end_user_country1, source_type_code, vendor_name, end_user_city, end_user_state, adjustment_type, delete_flag, product_sub_line, earliest_acceptable_date, reason_for_manual_adjustment, business_segment, -1*NEG_CM_AMOUNT, burdened_cost, rsm_nm, srep_nm, baan_po, earliest_acceptable_date_spain, oracle_po_number, inventory_item_id, organization_id, invoice_to_org_id, ship_to_org_id, code_combination_id, salesrep_id, NEG_counter+1,data_source
from g00103.booking_fact_temp left join lkp_cancellation_exists b on (order_header_id=CE_ORDER_HEADER_ID and order_line_id=CE_ORDER_LINE_ID)
where NEG_ORDER_LINE_ID is not null and NEG_ORDER_HEADER_ID is not null and (NET_QUANTITY <> NEG_NET_QUANTITY or NET_AMOUNT_FUNC_CURRENCY <> NEG_NET_AMOUNT_FUNC_CURRENCY 
or MANAGEMENT_ENTITY <> NEG_MANAGEMENT_ENTITY or (ORDER_STATUS_CODE <> NEG_ORDER_STATUS_CODE and NEG_ORDER_STATUS_CODE = 'ENTERED') 
or (LINE_STATUS_CODE <> NEG_LINE_STATUS_CODE and NEG_LINE_STATUS_CODE = 'ENTERED')
OR (ORDER_STATUS_CODE <> NEG_ORDER_STATUS_CODE and ORDER_STATUS_CODE = 'CANCELLED')
OR (LINE_STATUS_CODE <> NEG_LINE_STATUS_CODE and LINE_STATUS_CODE = 'CANCELLED')) AND b.CE_ORDER_HEADER_ID is null and b.CE_ORDER_LINE_ID is null""")

existing_record_c.createOrReplaceTempView("df_existing_record_c")

val semi_final = new_record.union(existing_record_c)

val existing_record_i = spark.sql("""select order_header_id,
order_line_id, order_number, line_number, catlog_number, catlog_description, customer_po_nbr, ship_to_customer_number, ship_to_customer_name, customer_group, ship_to_address_1, ship_to_address_2, ship_to_address_3, ship_to_address_4, ship_to_city, ship_to_state, ship_to_country, ship_to_zip_code, bill_to_customer_number, bill_to_customer_name, bill_to_address_1, bill_to_address_2, bill_to_address_3, bill_to_address_4, bill_to_city, bill_to_state, bill_to_country, bill_to_zip_code, sold_to_customer_number, sold_to_customer_name, sold_to_address_1, sold_to_address_2, sold_to_address_3, sold_to_address_4, sold_to_city, sold_to_state, sold_to_country, sold_to_zip_code, end_user_name, customer_request_date, order_receipt_date, ordered_date, order_entry_date, booked_date, promise_date, scheduled_ship_date, shipped_date, process_date, quote_date, product_class_code, sales_channel, ship_from_warehouse_name, ship_from_warehouse_code, management_entity, region, subregion, currency_code, net_quantity, unit_selling_price, net_amount_func_currency, material_cost, matarial_ovhd_cost, labor_cost, overhead_cost, osp_cost, commissions, freight, usd_conversion_rate, net_amount_usd, material_cost_usd, matarial_ovhd_cost_usd, labour_cost_usd, overhead_cost_usd, osp_cost_usd, freight_cost_usd, unit_weight, ship_method, sales_engineer_code, sales_engineer_name, payment_term, order_status_code, line_status_code, order_type, line_type, product_tier_1, product_tier_2, product_tier_3, product_tier_4, product_tier_5, product_tier_6, product_tier_7, product_tier_8, product_tier_9, product_tier_10, attribute1, attribute2, attribute3, attribute4, attribute5, approach_code, world_pole, go_class_code, office_taken_code, customer_group_code, customer_type_description, cesd_user_number, spa_bumber, major_line_code, ro_number, replenishment_source, stock_ship_leadtime, transaction_type, baan_order_number, actual_order_line_flag, sold_to_site_number, ship_to_site_number, bill_to_site_number, packing_instruction, fob_point_code,  current_flag, record_created_by, record_created_date, booking_status, input_data, report_process1, report_process2, report_process3, report_process4, report_process5, tag_history_report_process, ord_hdr_attribute8, ord_hdr_attribute19, ord_hdr_attribute1, span, plant_description, plant_manager_name, product_platform, product_segment, family, end_destination_country, end_destination_state, product_line, order_type_pulse, sold_to_state_code, sold_to_country_code, bill_to_country_code, bill_to_state_code, ship_to_country_code, item_type_code, importing_textfile, glo_ult_duns, standard_parent_customername, ge_global_duns, ge_duns_name, standard_customername, standard_customernumber, sm_name, sm_ohr, sales_rep, componentflag, business, productclassgroup, subsection, world_sub_pole, state_code, ship_date, shipstatecode, error_code, foreign_currency_code, manufacturing_description, product_platform_rma, product_group_rma, product_line_rma, product_class_rma, shipto_sitenumber, customer_number, ship_to_region, bill_to_region, ship_to_cust_group, ship_to_classify, bill_to_cust_group, bill_to_classify, cancelled_flag, foreign_currecy_rate, revised_channel, line_category_code, ship_to_sub_region, bill_to_sub_region, site, productdesc_busrpt, project_number, family_busrpt, subfamily_busrpt, pnl, orig_sys_document_ref, district_code, atm_code, attribute12, attribute13, option_number, shipment_number, end_user, end_user_country, end_user_region, end_user_country1, source_type_code, vendor_name, end_user_city, end_user_state, adjustment_type, delete_flag, product_sub_line, earliest_acceptable_date, reason_for_manual_adjustment, business_segment, cm_amount, burdened_cost, rsm_nm, srep_nm, baan_po, earliest_acceptable_date_spain, oracle_po_number, inventory_item_id, organization_id, invoice_to_org_id, ship_to_org_id, code_combination_id, salesrep_id, NEG_counter+2,data_source from g00103.booking_fact_temp
where NEG_ORDER_LINE_ID is not null and NEG_ORDER_HEADER_ID is not null and (NET_QUANTITY <> NEG_NET_QUANTITY or NET_AMOUNT_FUNC_CURRENCY <> NEG_NET_AMOUNT_FUNC_CURRENCY 
or MANAGEMENT_ENTITY <> NEG_MANAGEMENT_ENTITY or (ORDER_STATUS_CODE <> NEG_ORDER_STATUS_CODE and NEG_ORDER_STATUS_CODE = 'ENTERED') 
or (LINE_STATUS_CODE <> NEG_LINE_STATUS_CODE and NEG_LINE_STATUS_CODE = 'ENTERED')
OR (ORDER_STATUS_CODE <> NEG_ORDER_STATUS_CODE and ORDER_STATUS_CODE = 'CANCELLED')
OR (LINE_STATUS_CODE <> NEG_LINE_STATUS_CODE and LINE_STATUS_CODE = 'CANCELLED')) AND 
 order_status_code <> 'CANCELLED' and line_status_code <> 'CANCELLED'""")
val finale = semi_final.union(existing_record_i)
finale.createOrReplaceTempView("final_df")
spark.sql("cache table final_df")

val sql3 = s"""Insert into table g00103.iqp_order_booking_fact PARTITION (data_source) select * from final_df"""
spark.sql(sql3)

//finale.write.mode("overwrite").format("orc").partitionBy("data_source").saveAsTable("g00103.iqp_order_booking_fact")


    println("iqp_order_booking loaded successfully")
    spark.stop()
  }
}