import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.DataFrame
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import java.io.File
import org.apache.spark.storage.StorageLevel
import java.text.SimpleDateFormat
import java.lang.Math

object o_sales_to_csv
{
  val tableName = "g00103.sales_to_csv"
  val sConnect = new Utils.sparkConnection
  //val spark = sConnect.connect(tableName)
  val spark = SparkSession.builder
      .appName(tableName)
      .config("spark.some.config.option", "config-value")
//    .config("spark.sql.warehouse.dir", warehouseLocation)
//    .config("orc.compress","ZLIB")
      .enableHiveSupport()
      .getOrCreate()
      spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      spark.conf.set("spark.kryo.registrationRequired", "true")
      spark.conf.set("spark.scheduler.mode", "FAIR")
      spark.conf.set("spark.sql.orc.filterPushdown", "true")
      spark.conf.set("spark.storage.StorageLevel","MEMORY_ONLY_SER")
      spark.conf.set("spark.kryo.classesToRegister", "AuditTableSchema")
  spark.conf.set("spark.sql.autoBroadcastJoinThreshold",52428800)
def main(args: Array[String])={
val deBug = args.exists{ x => x.toLowerCase.contains("debug=true") }
      var isSuccess: Boolean = true
      var errorMessage: String = ""
      val batchNumber = spark.sparkContext.applicationId
      val beforeTimestamp: Long = System.currentTimeMillis()
      val formatter = java.text.NumberFormat.getIntegerInstance
      if (deBug) Utils.printLog(deBug,tableName + ": Debug is Enabled for batchNumber: " + batchNumber)
try {


val geography_df=spark.sql("select * from g00103.iqp_geography_dim where upper(region_code)<>'REGION_CODE'")

geography_df.repartition(1).write.mode("overwrite").option("sep","\u0011").option("header","false").csv("/tmp/iqp/readable_files/iqp_geography_dim__y")

val business_df=spark.sql("select * from g00103.iqp_business_hier_dim ")

business_df.repartition(1).write.mode("overwrite").option("sep","\u0011").option("header","false").csv("/tmp/iqp/readable_files/iqp_business_hier_dim__y")


val sales_agg_df = spark.sql("""select 
  regexp_replace(regexp_extract(requisitionnumber,'\\p{Print}*',0), '[\"]','') as requisitionnumber ,
  regexp_replace(regexp_extract(invoicenumber,'\\p{Print}*',0), '[\"]','') as invoicenumber ,
  regexp_replace(regexp_extract(customernumber,'\\p{Print}*',0), '[\"]','') as customernumber ,
  processdate ,
  regexp_replace(regexp_extract(catalognumber,'\\p{Print}*',0), '[\"]','') as catalognumber ,
  regexp_replace(regexp_extract(productclass,'\\p{Print}*',0), '[\"]','') as productclass ,
  regexp_replace(regexp_extract(customerponumber,'\\p{Print}*',0), '[\"]','') as customerponumber ,
  customerorderdate ,
  customerrequestdate ,
  currentscheduledate ,
  invoicedate ,
  shipdate ,
  regexp_replace(regexp_extract(plantcode,'\\p{Print}*',0), '[\"]','') as plantcode ,
  regexp_replace(regexp_extract(shippingwarehousecode,'\\p{Print}*',0), '[\"]','') as shippingwarehousecode ,
  regexp_replace(regexp_extract(site,'\\p{Print}*',0), '[\"]','') as site ,
  regexp_replace(regexp_extract(countryname,'\\p{Print}*',0), '[\"]','') as countryname ,
  regexp_replace(regexp_extract(statecode,'\\p{Print}*',0), '[\"]','') as statecode ,
  netamount ,
  netquantity ,
  material ,
  labor ,
  overhead ,
  commissions ,
  freight ,
  regexp_replace(regexp_extract(groupmanager,'\\p{Print}*',0), '[\"]','') as groupmanager ,
  regexp_replace(regexp_extract(regionalmanager,'\\p{Print}*',0), '[\"]','') as regionalmanager ,
  regexp_replace(regexp_extract(agent_1,'\\p{Print}*',0), '[\"]','') as agent_1 ,
  regexp_replace(regexp_extract(source,'\\p{Print}*',0), '[\"]','') as source ,
  regexp_replace(regexp_extract(revisedchannel,'\\p{Print}*',0), '[\"]','') as revisedchannel ,
  stockshipleadtime ,
  regexp_replace(regexp_extract(shiptocountryname,'\\p{Print}*',0), '[\"]','') as shiptocountryname ,
  regexp_replace(regexp_extract(shipto_country,'\\p{Print}*',0), '[\"]','') as shipto_country ,
  regexp_replace(regexp_extract(ship_state_code,'\\p{Print}*',0), '[\"]','') as ship_state_code ,
  regexp_replace(regexp_extract(shipto_sitenumber,'\\p{Print}*',0), '[\"]','') as shipto_sitenumber ,
  regexp_replace(regexp_extract(soldto_country,'\\p{Print}*',0), '[\"]','') as soldto_country ,
  regexp_replace(regexp_extract(soldto_state,'\\p{Print}*',0), '[\"]','') as soldto_state ,
  regexp_replace(regexp_extract(soldto_sitenumber,'\\p{Print}*',0), '[\"]','') as soldto_sitenumber ,
  regexp_replace(regexp_extract(soldto_city,'\\p{Print}*',0), '[\"]','') as soldto_city ,
  regexp_replace(regexp_extract(billto_sitenumber,'\\p{Print}*',0), '[\"]','') as billto_sitenumber ,
  regexp_replace(regexp_extract(end_destination_country,'\\p{Print}*',0), '[\"]','') as end_destination_country ,
  regexp_replace(regexp_extract(sales_engineer,'\\p{Print}*',0), '[\"]','') as sales_engineer ,
  regexp_replace(regexp_extract(salesengineercode,'\\p{Print}*',0), '[\"]','') as salesengineercode ,
  regexp_replace(regexp_extract(productclass_id,'\\p{Print}*',0), '[\"]','') as productclass_id ,
  regexp_replace(regexp_extract(productdescription,'\\p{Print}*',0), '[\"]','') as productdescription ,
  regexp_replace(regexp_extract(market,'\\p{Print}*',0), '[\"]','') as market ,
  regexp_replace(regexp_extract(family,'\\p{Print}*',0), '[\"]','') as family ,
  regexp_replace(regexp_extract(businesssegment,'\\p{Print}*',0), '[\"]','') as businesssegment ,
  regexp_replace(regexp_extract(customernumber_id,'\\p{Print}*',0), '[\"]','') as customernumber_id ,
  regexp_replace(regexp_extract(customername,'\\p{Print}*',0), '[\"]','') as customername ,
  regexp_replace(regexp_extract(customerclassification,'\\p{Print}*',0), '[\"]','') as customerclassification ,
  regexp_replace(regexp_extract(channel,'\\p{Print}*',0), '[\"]','') as channel ,
  regexp_replace(regexp_extract(customergroupcode,'\\p{Print}*',0), '[\"]','') as customergroupcode ,
  regexp_replace(regexp_extract(region,'\\p{Print}*',0), '[\"]','') as region ,
  regexp_replace(regexp_extract(sub_region,'\\p{Print}*',0), '[\"]','') as sub_region ,
  regexp_replace(regexp_extract(pandl,'\\p{Print}*',0), '[\"]','') as pandl ,
  regexp_replace(regexp_extract(subpandl,'\\p{Print}*',0), '[\"]','') as subpandl ,
  regexp_replace(regexp_extract(pandl_pole,'\\p{Print}*',0), '[\"]','') as pandl_pole ,
  regexp_replace(regexp_extract(pandl_productline,'\\p{Print}*',0), '[\"]','') as pandl_productline ,
  regexp_replace(regexp_extract(pandl_tier1,'\\p{Print}*',0), '[\"]','') as pandl_tier1 ,
  regexp_replace(regexp_extract(pandl_tier2,'\\p{Print}*',0), '[\"]','') as pandl_tier2 ,
  regexp_replace(regexp_extract(pandl_tier3,'\\p{Print}*',0), '[\"]','') as pandl_tier3 ,
  regexp_replace(regexp_extract(pandl_tier3_productline,'\\p{Print}*',0), '[\"]','') as pandl_tier3_productline ,
  regexp_replace(regexp_extract(pandl_tier3_pole,'\\p{Print}*',0), '[\"]','') as pandl_tier3_pole ,
  regexp_replace(regexp_extract(pandl_pole_worldpole,'\\p{Print}*',0), '[\"]','') as pandl_pole_worldpole ,
  regexp_replace(regexp_extract(plantcode_id,'\\p{Print}*',0), '[\"]','') as plantcode_id ,
  plantgroup_id ,
  regexp_replace(regexp_extract(plantdescription,'\\p{Print}*',0), '[\"]','') as plantdescription ,
  regexp_replace(regexp_extract(plantmanagername,'\\p{Print}*',0), '[\"]','') as plantmanagername ,
  regexp_replace(regexp_extract(plantmarket,'\\p{Print}*',0), '[\"]','') as plantmarket ,
  regexp_replace(regexp_extract(backlogplantdescription,'\\p{Print}*',0), '[\"]','') as backlogplantdescription ,
  regexp_replace(regexp_extract(backlogplantdescription_report,'\\p{Print}*',0), '[\"]','') as backlogplantdescription_report ,
  regexp_replace(regexp_extract(productclass_id_busrpt,'\\p{Print}*',0), '[\"]','') as productclass_id_busrpt ,
  regexp_replace(regexp_extract(catalognumber_busrpt,'\\p{Print}*',0), '[\"]','') as catalognumber_busrpt ,
  regexp_replace(regexp_extract(plantcode_busrpt,'\\p{Print}*',0), '[\"]','') as plantcode_busrpt ,
  regexp_replace(regexp_extract(family_busrpt,'\\p{Print}*',0), '[\"]','') as family_busrpt ,
  regexp_replace(regexp_extract(subfamily_busrpt,'\\p{Print}*',0), '[\"]','') as subfamily_busrpt ,
  regexp_replace(regexp_extract(productdescription_busrpt,'\\p{Print}*',0), '[\"]','') as productdescription_busrpt ,
  regexp_replace(regexp_extract(productline_busrpt,'\\p{Print}*',0), '[\"]','') as productline_busrpt ,
  regexp_replace(regexp_extract(businesssegment_busrpt,'\\p{Print}*',0), '[\"]','') as businesssegment_busrpt ,
  regexp_replace(regexp_extract(businesssubsegment_busrpt,'\\p{Print}*',0), '[\"]','') as businesssubsegment_busrpt ,
  regexp_replace(regexp_extract(customergroup,'\\p{Print}*',0), '[\"]','') as customergroup ,
  regexp_replace(regexp_extract(statename,'\\p{Print}*',0), '[\"]','') as statename ,
  regexp_replace(regexp_extract(zipcode,'\\p{Print}*',0), '[\"]','') as zipcode ,
  regexp_replace(regexp_extract(productline,'\\p{Print}*',0), '[\"]','') as productline ,
  regexp_replace(regexp_extract(linenbr,'\\p{Print}*',0), '[\"]','') as linenbr ,
  regexp_replace(regexp_extract(componentflag,'\\p{Print}*',0), '[\"]','') as componentflag ,
  regexp_replace(regexp_extract(ordertype,'\\p{Print}*',0), '[\"]','') as ordertype ,
  regexp_replace(regexp_extract(product_platform,'\\p{Print}*',0), '[\"]','') as product_platform ,
  regexp_replace(regexp_extract(product_line,'\\p{Print}*',0), '[\"]','') as product_line ,
  regexp_replace(regexp_extract(product_segment,'\\p{Print}*',0), '[\"]','') as product_segment ,
  regexp_replace(regexp_extract(business,'\\p{Print}*',0), '[\"]','') as business ,
  regexp_replace(regexp_extract(subsection,'\\p{Print}*',0), '[\"]','') as subsection ,
  regexp_replace(regexp_extract(productclassgroup,'\\p{Print}*',0), '[\"]','') as productclassgroup ,
  material_overhead ,
  osp_cost ,
  non_ge_part_cost ,
  regexp_replace(regexp_extract(baan_order_number,'\\p{Print}*',0), '[\"]','') as baan_order_number ,
  regexp_replace(regexp_extract(product_platform_rma,'\\p{Print}*',0), '[\"]','') as product_platform_rma ,
  regexp_replace(regexp_extract(productgroup_rma,'\\p{Print}*',0), '[\"]','') as productgroup_rma ,
  regexp_replace(regexp_extract(product_line_rma,'\\p{Print}*',0), '[\"]','') as product_line_rma ,
  regexp_replace(regexp_extract(product_class_rma,'\\p{Print}*',0), '[\"]','') as product_class_rma ,
  regexp_replace(regexp_extract(oracle_sale_order,'\\p{Print}*',0), '[\"]','') as oracle_sale_order ,
  regexp_replace(regexp_extract(project_number,'\\p{Print}*',0), '[\"]','') as project_number ,
  regexp_replace(regexp_extract(bill_to_city,'\\p{Print}*',0), '[\"]','') as bill_to_city ,
  regexp_replace(regexp_extract(bill_to_state,'\\p{Print}*',0), '[\"]','') as bill_to_state ,
  regexp_replace(regexp_extract(bill_to_country,'\\p{Print}*',0), '[\"]','') as bill_to_country ,
  regexp_replace(regexp_extract(order_status_code,'\\p{Print}*',0), '[\"]','') as order_status_code ,
  project_id ,
  draft_revenue_num ,
  draft_revenue_line_num ,
  regexp_replace(regexp_extract(input_source,'\\p{Print}*',0), '[\"]','') as input_source ,
  regexp_replace(regexp_extract(data_source,'\\p{Print}*',0), '[\"]','') as data_source ,
  record_creation_date ,
  regexp_replace(regexp_extract(report_process1,'\\p{Print}*',0), '[\"]','') as report_process1 ,
  regexp_replace(regexp_extract(order_type_pulse,'\\p{Print}*',0), '[\"]','') as order_type_pulse ,
  regexp_replace(regexp_extract(importing_textfile,'\\p{Print}*',0), '[\"]','') as importing_textfile ,
  regexp_replace(regexp_extract(management_entity,'\\p{Print}*',0), '[\"]','') as management_entity ,
  regexp_replace(regexp_extract(ship_to_cust_group,'\\p{Print}*',0), '[\"]','') as ship_to_cust_group ,
  regexp_replace(regexp_extract(ship_to_classify,'\\p{Print}*',0), '[\"]','') as ship_to_classify ,
  regexp_replace(regexp_extract(bill_to_cust_group,'\\p{Print}*',0), '[\"]','') as bill_to_cust_group ,
  regexp_replace(regexp_extract(bill_to_classify,'\\p{Print}*',0), '[\"]','') as bill_to_classify ,
  unit_selling_price ,
  regexp_replace(regexp_extract(ship_to_city,'\\p{Print}*',0), '[\"]','') as ship_to_city ,
  regexp_replace(regexp_extract(ship_to_region,'\\p{Print}*',0), '[\"]','') as ship_to_region ,
  regexp_replace(regexp_extract(bill_to_region,'\\p{Print}*',0), '[\"]','') as bill_to_region ,
  regexp_replace(regexp_extract(tag_history_report_process,'\\p{Print}*',0), '[\"]','') as tag_history_report_process ,
  regexp_replace(regexp_extract(end_user_name,'\\p{Print}*',0), '[\"]','') as end_user_name ,
  regexp_replace(regexp_extract(fob_point,'\\p{Print}*',0), '[\"]','') as fob_point ,
  regexp_replace(regexp_extract(type,'\\p{Print}*',0), '[\"]','') as type ,
  regexp_replace(regexp_extract(uom_code,'\\p{Print}*',0), '[\"]','') as uom_code ,
  regexp_replace(regexp_extract(invoice_currency_code,'\\p{Print}*',0), '[\"]','') as invoice_currency_code ,
  regexp_replace(regexp_extract(operating_unit_name,'\\p{Print}*',0), '[\"]','') as operating_unit_name ,
  unit_cost ,
  func_amount ,
  regexp_replace(regexp_extract(catalog_description,'\\p{Print}*',0), '[\"]','') as catalog_description ,
  gl_date ,
  regexp_replace(regexp_extract(cust_trx_all_org_id,'\\p{Print}*',0), '[\"]','') as cust_trx_all_org_id ,
  regexp_replace(regexp_extract(warehouse_name,'\\p{Print}*',0), '[\"]','') as warehouse_name ,
  regexp_replace(regexp_extract(industry_sector,'\\p{Print}*',0), '[\"]','') as industry_sector ,
  regexp_replace(regexp_extract(order_type_pulse1,'\\p{Print}*',0), '[\"]','') as order_type_pulse1 ,
  regexp_replace(regexp_extract(importing_textfile1,'\\p{Print}*',0), '[\"]','') as importing_textfile1 ,
  regexp_replace(regexp_extract(major_line_code,'\\p{Print}*',0), '[\"]','') as major_line_code ,
  regexp_replace(regexp_extract(ra_terms_name,'\\p{Print}*',0), '[\"]','') as ra_terms_name ,
  term_due_date ,
  regexp_replace(regexp_extract(approach_code,'\\p{Print}*',0), '[\"]','') as approach_code ,
  regexp_replace(regexp_extract(transaction_type,'\\p{Print}*',0), '[\"]','') as transaction_type ,
  regexp_replace(regexp_extract(office_taken_code,'\\p{Print}*',0), '[\"]','') as office_taken_code ,
  regexp_replace(regexp_extract(ship_method,'\\p{Print}*',0), '[\"]','') as ship_method ,
  regexp_replace(regexp_extract(district_code,'\\p{Print}*',0), '[\"]','') as district_code ,
  firstcustshipdate ,
  firstcustdeliverydate ,
  regexp_replace(regexp_extract(customertypedesc,'\\p{Print}*',0), '[\"]','') as customertypedesc ,
  regexp_replace(regexp_extract(cesdusernbr,'\\p{Print}*',0), '[\"]','') as cesdusernbr ,
  unitlistprice ,
  grosssales ,
  regexp_replace(regexp_extract(spanbr,'\\p{Print}*',0), '[\"]','') as spanbr ,
  regexp_replace(regexp_extract(shiptocustomernbr,'\\p{Print}*',0), '[\"]','') as shiptocustomernbr ,
  regexp_replace(regexp_extract(ledgercode,'\\p{Print}*',0), '[\"]','') as ledgercode ,
  regexp_replace(regexp_extract(salesaccount,'\\p{Print}*',0), '[\"]','') as salesaccount ,
  regexp_replace(regexp_extract(salescostcenter,'\\p{Print}*',0), '[\"]','') as salescostcenter ,
  regexp_replace(regexp_extract(atmcode,'\\p{Print}*',0), '[\"]','') as atmcode ,
  regexp_replace(regexp_extract(usercustomernumber,'\\p{Print}*',0), '[\"]','') as usercustomernumber ,
  regexp_replace(regexp_extract(sequencenumber,'\\p{Print}*',0), '[\"]','') as sequencenumber ,
  regexp_replace(regexp_extract(chargetocustomernumber,'\\p{Print}*',0), '[\"]','') as chargetocustomernumber ,
  regexp_replace(regexp_extract(ronumber,'\\p{Print}*',0), '[\"]','') as ronumber ,
  regexp_replace(regexp_extract(sub_product_line,'\\p{Print}*',0), '[\"]','') as sub_product_line ,
  regexp_replace(regexp_extract(unit_weight,'\\p{Print}*',0), '[\"]','') as unit_weight ,
  regexp_replace(regexp_extract(cash_discount,'\\p{Print}*',0), '[\"]','') as cash_discount ,
  order_receipt_date ,
  regexp_replace(regexp_extract(manufacturing_description,'\\p{Print}*',0), '[\"]','') as manufacturing_description ,
  regexp_replace(regexp_extract(spa_number,'\\p{Print}*',0), '[\"]','') as spa_number ,
  regexp_replace(regexp_extract(reason_code,'\\p{Print}*',0), '[\"]','') as reason_code ,
  regexp_replace(regexp_extract(bill_to_customer,'\\p{Print}*',0), '[\"]','') as bill_to_customer ,
  regexp_replace(regexp_extract(end_user_target,'\\p{Print}*',0), '[\"]','') as end_user_target ,
  regexp_replace(regexp_extract(firmware,'\\p{Print}*',0), '[\"]','') as firmware ,
  cust_req_date ,
  regexp_replace(regexp_extract(revised_customer,'\\p{Print}*',0), '[\"]','') as revised_customer ,
  bill_unit_code ,
  regexp_replace(regexp_extract(id_identity,'\\p{Print}*',0), '[\"]','') as id_identity ,
  regexp_replace(regexp_extract(ship_channel,'\\p{Print}*',0), '[\"]','') as ship_channel ,
  regexp_replace(regexp_extract(salesman_name,'\\p{Print}*',0), '[\"]','') as salesman_name ,
  regexp_replace(regexp_extract(shippingwarehouse,'\\p{Print}*',0), '[\"]','') as shippingwarehouse ,
  regexp_replace(regexp_extract(customer_number_concat,'\\p{Print}*',0), '[\"]','') as customer_number_concat ,
  conversion_rate ,
  regexp_replace(regexp_extract(batch_source_all_name,'\\p{Print}*',0), '[\"]','') as batch_source_all_name ,
  regexp_replace(regexp_extract(end_destination_state,'\\p{Print}*',0), '[\"]','') as end_destination_state ,
  regexp_replace(regexp_extract(end_destination_city,'\\p{Print}*',0), '[\"]','') as end_destination_city ,
  regexp_replace(regexp_extract(ship_to_customer_name,'\\p{Print}*',0), '[\"]','') as ship_to_customer_name ,
  regexp_replace(regexp_extract(ou,'\\p{Print}*',0), '[\"]','') as ou ,
  pod_date ,
  regexp_replace(regexp_extract(flow_status_code,'\\p{Print}*',0), '[\"]','') as flow_status_code ,
  regexp_replace(regexp_extract(type_name,'\\p{Print}*',0), '[\"]','') as type_name ,
  regexp_replace(regexp_extract(bill_to_customer_number,'\\p{Print}*',0), '[\"]','') as bill_to_customer_number ,
  regexp_replace(regexp_extract(interface_header_attribute1,'\\p{Print}*',0), '[\"]','') as interface_header_attribute1 ,
  shipping_date ,
  regexp_replace(regexp_extract(vendor_name,'\\p{Print}*',0), '[\"]','') as vendor_name ,
  regexp_replace(regexp_extract(end_user_country,'\\p{Print}*',0), '[\"]','') as end_user_country ,
  regexp_replace(regexp_extract(end_user_city,'\\p{Print}*',0), '[\"]','') as end_user_city ,
  regexp_replace(regexp_extract(end_user_state,'\\p{Print}*',0), '[\"]','') as end_user_state ,
  regexp_replace(regexp_extract(govt_nongovt,'\\p{Print}*',0), '[\"]','') as govt_nongovt ,
  regexp_replace(regexp_extract(order_line_source_type,'\\p{Print}*',0), '[\"]','') as order_line_source_type ,
  regexp_replace(regexp_extract(sales_order_currency,'\\p{Print}*',0), '[\"]','') as sales_order_currency ,
  regexp_replace(regexp_extract(sales_order_line_number,'\\p{Print}*',0), '[\"]','') as sales_order_line_number ,
  regexp_replace(regexp_extract(cw_business,'\\p{Print}*',0), '[\"]','') as cw_business ,
  regexp_replace(regexp_extract(cw_cost_of_goods_sold_acc,'\\p{Print}*',0), '[\"]','') as cw_cost_of_goods_sold_acc ,
  regexp_replace(regexp_extract(cw_end_customer_name,'\\p{Print}*',0), '[\"]','') as cw_end_customer_name ,
  cw_estimated_item_cost ,
  cw_est_material_cost ,
  cw_est_labour_cost ,
  cw_est_overhead_cost ,
  cw_est_outsdprocess_cost ,
  cw_est_material_ovh_cost ,
  regexp_replace(regexp_extract(cw_family_id,'\\p{Print}*',0), '[\"]','') as cw_family_id ,
  regexp_replace(regexp_extract(cw_go_schedule,'\\p{Print}*',0), '[\"]','') as cw_go_schedule ,
  regexp_replace(regexp_extract(cw_inco_terms,'\\p{Print}*',0), '[\"]','') as cw_inco_terms ,
  regexp_replace(regexp_extract(cw_invoice_type,'\\p{Print}*',0), '[\"]','') as cw_invoice_type ,
  regexp_replace(regexp_extract(cw_make_buy,'\\p{Print}*',0), '[\"]','') as cw_make_buy ,
  cw_mat_percentage_of_sales ,
  regexp_replace(regexp_extract(cw_order_origin,'\\p{Print}*',0), '[\"]','') as cw_order_origin ,
  regexp_replace(regexp_extract(cw_sales_ledger_account,'\\p{Print}*',0), '[\"]','') as cw_sales_ledger_account ,
  acctd_amount ,
  function_amount ,
  order_creation_date ,
  order_line_creation_date ,
  order_date ,
  regexp_replace(regexp_extract(operational_department,'\\p{Print}*',0), '[\"]','') as operational_department ,
  regexp_replace(regexp_extract(product_sub_line,'\\p{Print}*',0), '[\"]','') as product_sub_line ,
  earliest_acceptable_date ,
  regexp_replace(regexp_extract(reason_for_manual_adjustment,'\\p{Print}*',0), '[\"]','') as reason_for_manual_adjustment ,
  event_num ,
  regexp_replace(regexp_extract(business_segment,'\\p{Print}*',0), '[\"]','') as business_segment ,
  regexp_replace(regexp_extract(baan_po,'\\p{Print}*',0), '[\"]','') as baan_po ,
  earliest_acceptable_date_spain ,
  regexp_replace(regexp_extract(oracle_po_number,'\\p{Print}*',0), '[\"]','') as oracle_po_number ,
  regexp_replace(regexp_extract(adjustment_type,'\\p{Print}*',0), '[\"]','') as adjustment_type ,
  regexp_replace(regexp_extract(spa_pdq_num,'\\p{Print}*',0), '[\"]','') as spa_pdq_num ,
  regexp_replace(regexp_extract(ids_sales_order_line,'\\p{Print}*',0), '[\"]','') as ids_sales_order_line ,
  regexp_replace(regexp_extract(created_by,'\\p{Print}*',0), '[\"]','') as created_by 
from g00103.iqp_sales_agg 
where created_by in ('iqp_sales_agg_legacy', 'iqp_sales_agg_erp','iqp_sales_agg' )
""")

sales_agg_df.write.mode("overwrite").option("sep","\u0011").option("nullValue","\\N").csv("/tmp/iqp/readable_files/iqp_sales_agg__y")
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