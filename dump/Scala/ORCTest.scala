object ORCTest
{
  val tableName = "g00103.iqpORCTest"
  val tableNameCSV = "g00103.iqpORCTestCSV"
  val sConnect = new Utils.sparkConnection
  val spark = sConnect.connect(tableName)
  spark.sql("set spark.sql.shuffle.partitions=10")

  def main(args: Array[String])={

    val deBug = args.exists{ x => x.toLowerCase.contains("debug=true") }

    var isSuccess: Boolean = true
    var errorMessage: String = ""

    val batchNumber = spark.sparkContext.applicationId
    val beforeTimestamp: Long = System.currentTimeMillis()
    val formatter = java.text.NumberFormat.getIntegerInstance

    if (deBug) Utils.printLog(deBug,tableName + ": Debug is Enabled for batchNumber: " + batchNumber)

    try {

      val df_om = spark.sql("""
        select
        oeh.header_id,
        oel.line_id,
        ott.name,
        oeh.order_number,
        oeh.ordered_date,
        oeh.cust_po_number,
        oeh.flow_Status_code as order_flow_status,
        oeh.transactional_curr_Code,
        oel.line_number,
        oel.ordered_item,
        oel.schedule_ship_date,
        oel.order_quantity_uom,
        oel.ordered_quantity,
        oel.item_type_code,
        oel.actual_shipment_date,
        oel.flow_Status_Code as line_flow_status,
        oel.unit_selling_price
        from g00103.oe_order_headers_all oeh,
        g00103.oe_order_lines_all oel,
        g00103.oe_transaction_Types_tl ott
        where oeh.order_type_id = ott.transaction_Type_id
        and oeh.header_id = oel.header_id
      """)

      val df_om_csv = spark.sql("""
        select
        oeh.header_id,
        oel.line_id,
        ott.name,
        oeh.order_number,
        oeh.ordered_date,
        oeh.cust_po_number,
        oeh.flow_Status_code as order_flow_status,
        oeh.transactional_curr_Code,
        oel.line_number,
        oel.ordered_item,
        oel.schedule_ship_date,
        oel.order_quantity_uom,
        oel.ordered_quantity,
        oel.item_type_code,
        oel.actual_shipment_date,
        oel.flow_Status_Code as line_flow_status,
        oel.unit_selling_price
        from g00103.oe_order_headers_all__x oeh,
        g00103.oe_order_lines_all__x oel,
        g00103.oe_transaction_Types_tl__x ott
        where oeh.order_type_id = ott.transaction_Type_id
        and oeh.header_id = oel.header_id
      """)

      if (deBug) Utils.printLog(deBug, tableName + ": DF om:" + formatter.format(df_om.count))
      if (deBug) Utils.printLog(deBug, tableName + ": DF om_csv:" + formatter.format(df_om_csv.count))

      if (deBug) Utils.printLog(deBug,tableName + ": Reading ORC and Writing to ORC Table Count: ")
      df_om.repartition(10).write.format("orc").mode("overwrite").saveAsTable(tableName)
      if (deBug) Utils.printLog(deBug, tableName + ": Processing Completed ...")

      if (deBug) Utils.printLog(deBug,tableName + ": Reading ORC and Writing to CSV Table Count: ")
      df_om_csv.repartition(10).write.format("csv").mode("overwrite").saveAsTable(tableNameCSV)
      if (deBug) Utils.printLog(deBug, tableName + ": Processing Completed ...")

      if (deBug) Utils.printLog(deBug,tableName + ": Reading ORC and Writing to CSV Table Count: ")
      df_om.repartition(10).write.format("csv").mode("overwrite").saveAsTable(tableNameCSV)
      if (deBug) Utils.printLog(deBug, tableName + ": Processing Completed ...")

      if (deBug) Utils.printLog(deBug,tableName + ": Reading CSV and Writing to ORC Table Count: ")
      df_om_csv.repartition(10).write.format("orc").mode("overwrite").saveAsTable(tableName)
      if (deBug) Utils.printLog(deBug, tableName + ": Processing Completed ...")} catch {
  case e: Throwable => {
    isSuccess = false
    errorMessage = e.getMessage()
    if (deBug) Utils.printLog(deBug, tableName + ": Error in Processing: " + errorMessage)
   }
}
finally {
  val status = if (isSuccess) "SUCCESS" else "FAILED"
  if (deBug) Utils.printLog(deBug, tableName + ": Showing audit table with status: " + status)
  spark.stop
}
}
}



/*
ORC Time: 27s
CSV Time:
2017-10-05 04:54:19: g00103.iqpORCTest: Debug is Enabled for batchNumber: application_1504998078054_16542
2017-10-05 04:54:40: g00103.iqpORCTest: DF om:3,705,127
2017-10-05 04:54:58: g00103.iqpORCTest: DF om_csv:3,705,127
2017-10-05 04:54:58: g00103.iqpORCTest: Writing to ORC Table Count:
2017-10-05 04:55:25: g00103.iqpORCTest: Processing Completed ...
2017-10-05 04:55:25: g00103.iqpORCTest: Writing to CSV Table Count:
2017-10-05 04:55:56: g00103.iqpORCTest: Processing Completed ...
2017-10-05 04:55:56: g00103.iqpORCTest: Showing audit table with status: SUCCESS
*/
