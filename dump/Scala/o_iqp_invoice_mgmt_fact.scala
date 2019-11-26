import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.DataFrame
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import java.io.File
import org.apache.spark.storage.StorageLevel
import java.text.SimpleDateFormat

object o_iqp_invoice_mgmt_fact
{
  val tableName = "g00103.iqp_invoice_mgmt_fact"
  val sConnect = new Utils.sparkConnection
  val spark = sConnect.connect(tableName)

def main(args: Array[String])={
  
val deBug = args.exists{ x => x.toLowerCase.contains("debug=true") }

      var isSuccess: Boolean = true
      var errorMessage: String = ""
      val batchNumber = spark.sparkContext.applicationId
      val beforeTimestamp: Long = System.currentTimeMillis()
      val formatter = java.text.NumberFormat.getIntegerInstance
      if (deBug) Utils.printLog(deBug,tableName + ": Debug is Enabled for batchNumber: " + batchNumber)

    try {

spark.sql("set spark.sql.shuffle.partitions=10")
spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
spark.sql("set hive.exec.dynamic.partition=true")
//Added by anurag 

		spark.udf.register("getBusinessSegment", (ORG_UNIT_NAME:String,BUSINESSSEGMENT:String) => { 
val arr1 = Array("IO MULTILIN CA","IO MULTILIN PR","IO MULTILIN US","IO CLEARWATER","IO FT.EDWARD","IO MT.JULIET","IO SOMERSWORTH","IO LARGO","IO MONTREAL","IO KELMAN US","IO KELMAN UK")
val expr2 = if(arr1.contains(ORG_UNIT_NAME) == false) "T&D Products" else if(BUSINESSSEGMENT == null || (BUSINESSSEGMENT=="")) "OTHERS" else null
val expr3 = if(ORG_UNIT_NAME == null) "T&D Products" else expr2
expr3
           })
		   
		   spark.udf.register("getreport_process",(gl_date:Int,now:Int,now1:Int,hour:Int,account_set_flag:String,management_entity:String,name:String,cust_trx_all_org_id:String,cust_trx_lines_all_description:String,inventory_item_id:String,item_type_code:String,warehouse_name:String) => {
val arr1=Array("19RB","19RU","19RC","J74","J7P")
val arr2=Array("PROJECTS INVOICE","INTERCOMPANY")
val arr3=Array("872","2893","3113")
val arr4=Array("STANDARD","CONFIG")
val expr5=if( gl_date > now ) "Invoice Reporting" else "Pulse Reporting"
val expr4=if(  gl_date > now1  && hour < 12 ) "Invoice Reporting" else expr5
val expr3=if( arr4.contains(item_type_code) == true && warehouse_name == "IO ROCHESTER")  "Rejected" else expr5
val expr2=if( arr3.contains( cust_trx_all_org_id) == true && cust_trx_lines_all_description == "FREIGHT" && inventory_item_id == null) "Rejected" else expr3
val expr1=if( (Option(gl_date).getOrElse("").toString == null) || (account_set_flag != "N") || (arr1.contains(management_entity) == true) || (arr2.contains(name) == true) ) "Rejected" else expr2
expr1
})
 
         
 

          // GL Rates Temp View for MOR (1000) Rate Type
          val df_gl = spark.sql("""
            select from_currency, conversion_date,  conversion_rate
            from  g00103.gl_daily_rates
            where to_currency = 'USD'
            and conversion_type = '1000'
          """).createOrReplaceTempView("glRate1")

          spark.sql("cache table glRate1")

          if (deBug) Utils.printLog(deBug,tableName + ": DF gl:" + formatter.format(spark.sql("select 1 from glRate1").count))
			  
		  

          //GL Code code_combination_id
          val df_ccid = spark.sql("""
            select code_combination_id
               , concat(segment1,'-',segment2,'-',segment3,'-',segment4,'-',segment5,'-',segment6,'-',segment7,'-',segment8) gl_account
               , segment1 legal_entity
               , segment2 account
               , segment3 gl_function
               , segment4 analytical
               , segment5 me
            from g00103.gl_code_combinations
          """).cache
          val df_charge_acc = df_ccid.createOrReplaceTempView("charge_acc")

        //Customer Bill-To Dim
        val df_cust_billto_dim = spark.sql("""
          select cust_account_id cust_billto_cust_account_id
          , cust_site_uses_site_use_id cust_billto_cust_site_uses_site_use_id
          , customer_group
          , customer_group bill_to_cust_group
          , bill_to_region
          , bill_to_sub_region
          , party_name bill_to_customer_name
          , account_number bill_to_customer_number
          , customer_classification bill_to_classify
          , locations_address1 bill_to_address1
          , locations_address2 bill_to_address2
          , locations_address3 bill_to_address3
          , locations_address4 bill_to_address4
          , loc_city bill_to_city
          , loc_state bill_to_state
          , loc_country bill_to_country
          , loc_county bill_to_county
          , loc_postal_code bill_to_postal_code
          , loc_province bill_to_province
          from g00103.iqp_customer_dim
          where site_use_code = 'BILL_TO'
        """).createOrReplaceTempView("cust_billto_dim")

        spark.sql("cache table cust_billto_dim")

        if (deBug) Utils.printLog(deBug, tableName + ": DF cust_billto_dim:" + formatter.format(spark.sql("select 1 from cust_billto_dim").count))

        //Customer Ship-To Dim
        val df_cust_shipto_dim = spark.sql("""
          select cust_account_id cust_shipto_cust_account_id
          , cust_site_uses_site_use_id cust_shipto_cust_site_uses_site_use_id
          --, customer_group
          , customer_group ship_to_cust_group
          , ship_to_region region
          , ship_to_region
          , ship_to_sub_region
          , ship_to_sub_region subregion
          , party_name ship_to_customer_name
          , account_number ship_to_customer_number
          , customer_classification ship_to_classify
          , locations_address1 ship_to_address1
          , locations_address2 ship_to_address2
          , locations_address3 ship_to_address3
          , locations_address4 ship_to_address4
          , loc_city ship_to_city
          , loc_state ship_to_state
          , loc_country ship_to_country
          , loc_county ship_to_county
          , loc_postal_code ship_to_postal_code
          , loc_province ship_to_province
          from g00103.iqp_customer_dim
          where site_use_code = 'SHIP_TO'
        """).createOrReplaceTempView("cust_shipto_dim")

        spark.sql("cache table cust_shipto_dim")

        if (deBug) Utils.printLog(deBug, tableName + ": DF cust_shipto_dim:" + formatter.format(spark.sql("select 1 from cust_shipto_dim").count))

        //Item Organization Dim
        val df_item_dim = spark.sql("""
          select distinct
		       msi.material_cost item_dim_material_cost
             , coalesce(msi.overhead_cost, 0) item_dim_overhead_cost
             , coalesce(msi.material_overhead_cost,0) item_dim_material_overhead_cost
             , coalesce(msi.outside_processing_cost, 0) item_dim_outside_processing_cost
             , coalesce(msi.resource_cost, 0) item_dim_labor_cost
             , cast (msi.inventory_item_id as bigint) item_dim_inventory_item_id
             , cast (msi.organization_id as bigint) item_dim_organization_id
             , msi.mtl_system_items_b_description item_long_description
             , msi.product_number product_number
             , msi.planning_make_buy_code_name cw_make_buy
             , coalesce(cic.item_cost,0) cw_estimated_item_cost
             , coalesce(cic.material_cost,0) cw_est_material_cost
             , coalesce(cic.material_overhead_cost,0) cw_est_material_ovh_cost
             , coalesce(cic.resource_cost,0) cw_est_labour_cost
             , coalesce(cic.outside_processing_cost,0) cw_est_outsdprocess_cost
             , coalesce(cic.overhead_cost,0) cw_est_overhead_cost
          from g00103.iqp_itemwarehouse_dim msi
             left outer join g00103.cst_item_costs cic on (cic.inventory_item_id = msi.inventory_item_id
                                                       and cic.organization_id = msi.organization_id
                                                       and cic.cost_type_id = 1063)
        """).createOrReplaceTempView("item_dim")

        spark.sql("cache table item_dim")

        if (deBug) Utils.printLog(deBug, tableName + ": DF item_dim:" + formatter.format(spark.sql("select 1 from item_dim").count))

        val set_id = spark.sql("""
          select  mcs.category_set_id
          from g00103.mtl_category_sets mcs
          where mcs.category_set_name ='PD Product Line Category'
        """).first.getDouble(0)

        val df_item_cat = spark.sql(f"""
          select
             mc.segment1  as cw_business,
             mc.segment6  as cw_go_schedule,
             mc.segment9  as cw_family_id,
             mic.inventory_item_id  item_cat_inventory_item_id,
             mic.organization_id item_cat_organization_id
          from g00103.mtl_item_categories mic
               inner join g00103.mtl_categories mc on (mic.category_id = mc.category_id)
          where mic.category_set_id = $set_id%.1f
        """).createOrReplaceTempView("item_cat")

        if (deBug) Utils.printLog(deBug, tableName + ": DF item_cat:" + formatter.format(spark.sql("select 1 from item_cat").count))

        //Invoice Terms
        val df_inv_terms = spark.sql("""
          select
            row_id
            ,term_id
            ,printing_lead_days printing_lead_days
            ,name ra_terms_name
          from g00103.ra_terms_vl
        """).createOrReplaceTempView("inv_term")

        if (deBug) Utils.printLog(deBug, tableName + ": DF inv_term:" + formatter.format(spark.sql("select 1 from inv_term").count))

        //Invoice Transaction Types
        val df_trx_type = spark.sql("""
          select
            cust_trx_type_id
            ,org_id cust_trx_types_all_org_id
            ,name cust_trx_types_all_name
            ,type type
			,name name
          from g00103.ra_cust_trx_types_all
        """).createOrReplaceTempView("trx_type")

        if (deBug) Utils.printLog(deBug, tableName + ": DF trx_type:" + formatter.format(spark.sql("select 1 from trx_type").count))

        //Invoice Batch Sources
        val df_batch_src = spark.sql("""
          select
            batch_source_id
            ,org_id batch_sources_all_org_id
            ,name batch_sources_all_name
          from g00103.ra_batch_sources_all
        """).createOrReplaceTempView("batch_src")

        if (deBug) Utils.printLog(deBug, tableName + ": DF batch_src:" + formatter.format(spark.sql("select 1 from batch_src").count))

        //Invoice Header
        val df_inv_h = spark.sql("""
            select
              customer_trx_id
              ,customer_trx_id invoice_header_id
              ,primary_salesrep_id primary_salesrep_id
              ,sold_to_customer_id sold_to_customer_id
              ,bill_to_customer_id bill_to_customer_id
              ,bill_to_site_use_id bill_to_site_use_id
              ,ship_to_customer_id ship_to_customer_id
              ,ship_to_contact_id ship_to_contact_id
              ,ship_to_site_use_id ship_to_site_use_id
              ,invoice_currency_code invoice_currency_code
              ,remit_to_address_id remit_to_address_id
              ,related_customer_trx_id related_customer_trx_id
              ,invoicing_rule_id invoicing_rule_id
              ,customer_bank_account_id customer_bank_account_id
              ,paying_customer_id paying_customer_id
              ,paying_site_use_id paying_site_use_id
              ,doc_sequence_id doc_sequence_id
              ,org_id cust_trx_all_org_id
              ,trx_number invoice_number
              ,reason_code cust_trx_all_reason_code
              ,printing_option printing_option
              ,printing_count printing_count
              ,printing_pending printing_pending
              ,purchase_order purchase_order
              ,purchase_order_revision purchase_order_revision
              ,customer_reference customer_reference
              ,comments cust_trx_line_gl_dist_comments
              ,internal_notes internal_notes
              ,exchange_rate_type exchange_rate_type
              ,exchange_rate exchange_rate
              ,last_printed_sequence_num last_printed_sequence_num
              ,credit_method_for_rules credit_method_for_rules
              ,credit_method_for_installments credit_method_for_installments
              ,ship_via ship_via
              ,waybill_number waybill_number
              ,fob_point fob_point
              ,interface_header_context interface_header_context
              ,recurred_from_trx_number recurred_from_trx_number
              ,status_trx status_trx
              ,doc_sequence_value doc_sequence_value
              ,approval_code approval_code
              ,old_trx_number old_trx_number
              ,ct_reference ct_reference
              ,ship_date_actual ship_date_actual
              ,end_date_commitment end_date_commitment
              ,start_date_commitment start_date_commitment
              ,exchange_date exchange_date
              ,printing_last_printed printing_last_printed
              ,wh_update_date cust_trx_all_wh_update_date
              ,customer_reference_date customer_reference_date
              ,trx_date invoice_date
              ,term_due_date term_due_date
              ,printing_original_date printing_original_date
              ,purchase_order_date purchase_order_date
              ,complete_flag complete_flag
              ,default_tax_exempt_flag default_tax_exempt_flag
              ,interface_header_attribute1 interface_header_attribute1
              ,interface_header_attribute6
              ,bill_to_contact_id bill_to_contact_id
              ,batch_id batch_id
              ,rct.attribute3 cust_trx_all_attribute3
              ,batch_src.*
              ,trx_type.*
              ,inv_term.*
              ,cust_billto_dim.*
              ,cust_shipto_dim.*
              ,ou.org_unit_name organization_name
              ,isd.salesrep_name salesman_name
              ,isd.salesrep_name salesman_engineer
            from g00103.ra_customer_trx_all rct
                inner join batch_src on (batch_src.batch_source_id = rct.batch_source_id
                                     and batch_src.batch_sources_all_org_id = rct.org_id)
                inner join trx_type on (trx_type.cust_trx_type_id = rct.cust_trx_type_id
                                    and trx_type.cust_trx_types_all_org_id = rct.org_id)
                inner join inv_term on (inv_term.term_id = rct.term_id)
                inner join cust_billto_dim on (cust_billto_dim.cust_billto_cust_account_id = rct.bill_to_customer_id
                                           and cust_billto_dim.cust_billto_cust_site_uses_site_use_id = rct.bill_to_site_use_id)
                left outer join cust_shipto_dim on (cust_shipto_dim.cust_shipto_cust_account_id = rct.ship_to_customer_id
                                                and cust_shipto_dim.cust_shipto_cust_site_uses_site_use_id = rct.ship_to_site_use_id)
                inner join g00103.iqp_organization_dim ou on (ou.organization_id = rct.org_id)
                left outer join g00103.iqp_salesrep_dim isd on (isd.salesrep_id = rct.primary_salesrep_id
                                                            and isd.organization_id = rct.org_id)
        """).createOrReplaceTempView("inv_h")

        spark.sql("cache table inv_h")
        spark.sql("uncache table cust_billto_dim")
        spark.sql("uncache table cust_shipto_dim")

        if (deBug) Utils.printLog(deBug, tableName + ": DF inv_h:" + formatter.format(spark.sql("select 1 from g00103.inv_h").count))

        //Invoice Lines
        val df_inv_l = spark.sql("""
          select
            rctl.customer_trx_line_id
            ,rctl.customer_trx_id cust_trx_lines_all_customer_trx_id
            ,rctl.inventory_item_id inventory_item_id
            ,rctl.previous_customer_trx_id previous_customer_trx_id
            ,rctl.previous_customer_trx_line_id previous_customer_trx_line_id
            ,rctl.accounting_rule_id accounting_rule_id
            ,rctl.link_to_cust_trx_line_id link_to_cust_trx_line_id
            ,rctl.vat_tax_id vat_tax_id
            ,rctl.tax_exemption_id tax_exemption_id
            ,rctl.memo_line_id memo_line_id
            ,rctl.org_id cust_trx_lines_all_org_id
            ,rctl.warehouse_id warehouse_id
            ,rctl.line_number line_number
            ,rctl.reason_code cust_trx_lines_all_reason_code
            ,rctl.quantity_ordered quantity_ordered
            ,rctl.quantity_credited quantity_credited
            ,rctl.quantity_invoiced quantity_invoiced
            ,rctl.unit_standard_price unit_standard_price
            ,rctl.sales_order sales_order
            ,rctl.sales_order_line sales_order_line
            ,rctl.accounting_rule_duration accounting_rule_duration
            ,rctl.line_type line_type
            ,rctl.interface_line_context interface_line_context
            ,rctl.interface_line_attribute6
            ,rctl.sales_order_source sales_order_source
            ,rctl.extended_amount extended_amount
            ,rctl.tax_precedence tax_precedence
            ,rctl.tax_rate tax_rate
            ,rctl.autorule_duration_processed autorule_duration_processed
            ,rctl.uom_code uom_code
            ,rctl.autotax autotax
            ,rctl.last_period_to_credit last_period_to_credit
            ,rctl.tax_exempt_number tax_exempt_number
            ,rctl.tax_exempt_reason_code tax_exempt_reason_code
            ,rctl.tax_vendor_return_code tax_vendor_return_code
            ,rctl.gross_unit_selling_price gross_unit_selling_price
            ,rctl.gross_unit_selling_price unit_selling_price
            ,rctl.gross_extended_amount gross_extended_amount
            ,rctl.taxable_amount taxable_amount
            ,rctl.invoiced_line_acctg_level invoiced_line_acctg_level
            ,rctl.translated_description translated_description
            ,rctl.description cust_trx_lines_all_description
            ,rctl.rule_end_date rule_end_date
            ,rctl.wh_update_date cust_trx_lines_wh_update_date
            ,rctl.sales_order_date sales_order_date
            ,rctl.rule_start_date rule_start_date
            ,rctl.autorule_complete_flag autorule_complete_flag
            ,rctl.tax_exempt_flag tax_exempt_flag
            ,rctl.amount_includes_tax_flag amount_includes_tax_flag
            ,rctl.interface_line_attribute1 order_number
            ,rctl.revenue_amount revenue_amount
            ,rctl.revenue_amount amount
            ,rctl.revenue_amount total_invoice_line_amount
            ,item_dim.item_dim_material_cost * quantity_ordered material_cost
            ,item_dim.item_dim_material_overhead_cost * quantity_ordered material_overhead
            ,item_dim.item_dim_outside_processing_cost * quantity_ordered osp_cost
            ,item_dim.item_dim_overhead_cost * quantity_ordered overhead_cost
            ,item_dim.item_dim_labor_cost * quantity_ordered labor_cost
            ,item_dim.cw_est_overhead_cost
            ,item_dim.cw_est_material_cost
            ,item_dim.cw_est_labour_cost
            ,item_dim.cw_estimated_item_cost
            ,item_dim.cw_est_outsdprocess_cost
            ,item_dim.cw_est_material_ovh_cost
            ,item_dim.cw_make_buy
            ,item_dim.item_long_description
            ,item_dim.product_number
            ,org.org_unit_name warehouse_name
            ,item_cat.cw_business
            ,item_cat.cw_go_schedule
            ,item_cat.cw_family_id
            ,omf.productclassgroup
            ,omf.product_line productline
            ,omf.product_sub_line
            ,omf.product_class
            ,omf.product_platform_item product_platform
            ,omf.product_segment
            ,omf.management_entity
            ,omf.business_segment
            ,omf.order_type_pulse
            ,omf.item_type_code
            ,omf.management_entity producing_plant
          from g00103.ra_customer_trx_lines_all rctl
              left outer join item_dim on (item_dim.item_dim_inventory_item_id = rctl.inventory_item_id
                                       and item_dim.item_dim_organization_id = rctl.warehouse_id)
              left outer join item_cat on (item_cat.item_cat_inventory_item_id = rctl.inventory_item_id
                                       and item_cat.item_cat_organization_id = rctl.warehouse_id)
              left outer join g00103.iqp_organization_dim org on (org.organization_id = rctl.warehouse_id)
              left outer join g00103.iqp_order_mgmt_fact omf on (rctl.interface_line_attribute6 = omf.line_id)
			  where rctl.line_type='LINE'
        """).createOrReplaceTempView("inv_l")
        // Join sales_order using interface_header_attribute1 = order_number and interface_line_attribute6 = LINE_ID
        // and interface_line_context = 'ORDER ENTRY'

        spark.sql("cache table inv_l")
        spark.sql("uncache table item_dim")

        if (deBug) Utils.printLog(deBug, tableName + ": DF inv_l:" + formatter.format(spark.sql("select 1 from g00103.inv_l").count))

      //Invoice Distribution
       val df_inv_d = spark.sql("""
         select
            cust_trx_line_gl_dist_id
            ,customer_trx_line_id gl_dist_all_customer_trx_line_id
            ,rctld.code_combination_id code_combination_id
            ,set_of_books_id set_of_books_id
            ,request_id request_id
            ,program_application_id program_application_id
            ,program_id program_id
            ,posting_control_id posting_control_id
            ,org_id cust_trx_line_gl_dist_org_id
            ,acctd_amount acctd_amount
            ,percent_1 percent
            ,account_class account_class
            ,concatenated_segments concatenated_segments
            ,comments cust_trx_all_comments
            ,gl_posted_date gl_posted_date
            ,gl_date gl_date
            ,original_gl_date original_gl_date
            ,account_set_flag account_set_flag
            ,rec_offset_flag rec_offset_flag
            ,latest_rec_flag latest_rec_flag
            ,gl_date conversion_date
            ,last_update_date gl_dist_all_last_update_date
            ,charge_acc.gl_account cw_sales_ledger_account
			,charge_acc.me as segment5_me
         from g00103.ra_cust_trx_line_gl_dist_all rctld
           left outer join charge_acc on (rctld.code_combination_id = charge_acc.code_combination_id)
		   where account_class = 'REV'
       """).createOrReplaceTempView("inv_d")

       spark.sql("cache table inv_d")

       if (deBug) Utils.printLog(deBug, tableName + ": DF inv_d:" + formatter.format(spark.sql("select 1 from inv_d").count))

         val dfRaw = spark.sql("""
           select inv_h.*
                  ,inv_l.*
                  ,inv_d.*
                  ,glRate1.conversion_rate conversion_rate
                  ,'iqp_invoice_mgmt_fact' record_inserted_by
                  ,'iqp_invoice_mgmt_fact' record_updated_by
                  ,current_date record_inserted_date
                  ,current_date record_updated_date
                  ,'' span
                  ,'' componentflag
                  ,'' cw_cost_of_goods_sold_acc
                  ,'' cw_inco_terms
                  ,'' cw_mat_percentage_of_sales
                  ,'' family
                  ,'' market
                  ,'' productclass_id
                  ,'' productdescription
                  ,'' approach_code
                  ,'' atm_code
                  ,'' baan_po
                  ,'' bill_unit_code
                  ,'' cash_discount
                  ,'' cesd_user_number
                  ,'' channel_id
                  ,'' charge_to_customer_number
                  ,'' commision
                  ,'' current_schedule_date
                  ,'' customer_group_code
                  ,'' customer_order_date
                  ,'' customer_po_nbr
                  ,'' customer_request_date
                  ,'' customer_type_description
                  ,'' district_code
                  ,'' earliest_acceptable_date
                  ,'' end_destination_country
                  ,'' family_busrpt
                  ,'' freight
                  ,'' frst_cust_delivery_date
                  ,'' frst_cust_ship_date
                  ,'' gross_sales
                  ,'' id_identity
                  ,'' importing_textfile
                  ,'' ledger_code
                  ,'' major_line_code
                  ,'' manufacturing_description
                  ,'' office_taken_code
                  ,'' oracle_order_number
                  ,'' order_receipt_date
                  ,'' order_status_code
                  ,'' order_type
                  ,'' process_date
                  ,'' product_class_rma
                  ,'' product_line_rma
                  ,'' product_platform_rma
                  ,'' project_number
                  ,'' ro_number
                  ,'' sales_account
                  ,'' sales_cost_centre
                  ,'' sales_engineer_code
                  ,'' sales_engineer_name
                  ,'' sequence_number
                  ,'' ship_method
                  ,'' shippingwarehousecode
                  ,'' site
                  ,'' spa_number
                  ,'' subsection
                  ,'' transaction_type
                  ,'' unit_list_price
                  ,'' unit_weight
                  ,'' user_customer_number
                  ,'' plant_description
                  ,'' plant_manager_name
				   --added by ac
				  ,'' account_number_buyer,
'' baan_order_number,
'' billto_sitenumber,
'' cash_discounts,
'' city_name,
'' cmf_compliance_code,
'' current_ship_date,
'' customer_po_date,
'' delivery_line_item,
'' delivery_no,
'' dist_ch_id,
'' end_destination_state,
'' end_user_name,
'' foreign_currency_net_amount,
'' go_class_code,
'' invoice_prefix,
'' invoice_type,
'' net_quantity,
'' non_ge_part_cost,
'' overhead,
'' product_group_rma,
'' productclass,
'' replenishment_source,
'' replinisment_source,
'' requisition_number,
'' revised_channel,
'' sales_channel,
'' sales_engineer,
'' salesmanagercode,
'' shipstatecode,
'' shipto_sitenumber,
'' soldto_city,
'' soldto_sitenumber,
'' state_name,
'' terms_code_description,
'' title_transfer_terms,
'' world_pole    , 
getreport_process(nvl(cast(unix_timestamp(nvl(inv_d.gl_date,'')) as Int),-1),cast(unix_timestamp(current_timestamp) as Int),cast(unix_timestamp(date_sub(current_timestamp,1)) as int),cast(hour(current_timestamp) as Int),
                  nvl(inv_d.account_set_flag,''),nvl(inv_l.management_entity,''),nvl(upper(inv_h.name),''),nvl(cust_trx_all_org_id,''),nvl(upper(cust_trx_lines_all_description),''),
				  nvl(inv_l.inventory_item_id,''),nvl(upper(inv_l.item_type_code),''),nvl(upper(inv_l.warehouse_name),''))
 as report_process1,                 
'' sold_to_country,
'' sold_to_state,
'' tag_history_report_process,
'' cw_end_customer_name,

getbusinesssegment(nvl(upper(od.ORG_UNIT_NAME),''),nvl(trim(lcl.BUSINESSSEGMENT),'')) as BUSINESSSEGMENT,
bhd.PLANTCODE_ID as Plantcode_id
,1 data_source

           from inv_h inv_h
             inner join inv_l inv_l on (inv_h.customer_trx_id = inv_l.cust_trx_lines_all_customer_trx_id)
             inner join inv_d on (inv_l.customer_trx_line_id = inv_d.gl_dist_all_customer_trx_line_id)
             left outer join glRate1 on (glRate1.from_currency = inv_h.invoice_currency_code
                                     and glRate1.conversion_date = inv_d.gl_date)
			  left join g00103.IQP_ORGANIZATION_DIM od on(od.ORGANIZATION_ID = inv_l.WAREHOUSE_ID)
             left join
             (
SELECT 
       BUSINESSSEGMENT as BUSINESSSEGMENT,
       MANAGEMENT_ENTITY as MANAGEMENT_ENTITY,
       INVENTORY_ITEM_ID as INVENTORY_ITEM_ID,
       substr(PLANTCODE, 1, 4) as PLANTCODE
       FROM g00103.IQP_LEGACY_CATALOG_LIST
       WHERE RECORD_INSERTED_BY = 'SPARK_o_g00103.IQP_legacy_catalog_list'
      )lcl on(lcl.INVENTORY_ITEM_ID=inv_l.INVENTORY_ITEM_ID and lcl.PLANTCODE=inv_l.WAREHOUSE_ID) left join
	  
	  g00103.iqp_business_hier_dim bhd on((case
                                       when UPPER(inv_l.warehouse_name) in('IO MULTILIN CA','IO MULTILIN PR','IO MULTILIN US')  
                                        then case when lcl.MANAGEMENT_ENTITY is null or trim(lcl.MANAGEMENT_ENTITY)='' then inv_d.segment5_me else lcl.MANAGEMENT_ENTITY end
                                        else inv_d.segment5_me end)= bhd.PLANTCODE_ID) 
	  
	  
          """).createOrReplaceTempView("raw")

          val dfFinal = spark.sql("select * from raw")



dfFinal.write.mode("overwrite").format("orc").partitionBy("data_source").saveAsTable("g00103.iqp_invoice_mgmt_fact")



		   

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
