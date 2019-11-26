/*
// This program checks for Tables listed in g00103.iqp_table_listings for uniqueness
// g00103.iqp_table_listings ->
// table_name                   unique_constraint                  enabled_flag
// -------------------------------------------------------------------------------
// g00103.oe_order_lines_all    line_id                            Y
// g00103.oe_order_headers_all  header_id                          Y
// g00103.mtl_serial_numbers    serial_number, inventory_item_id   Y

CREATE TABLE g00103.iqp_table_listings
(
  table_name string,
  table_type string,
  unique_constraint string,
  enabled_flag string
)
STORED as ORC;

Usage: iqp_table_check_daemon [debug=true] [table_type {LIKE,=} 'TableType'] [table_name {LIKE,=} 'TableName']
*/

import scala.collection.mutable.ListBuffer
import java.text.SimpleDateFormat

object iqp_table_check_daemon
{
  val tableName = "g00103.iqp_table_check_daemon"
  val sConnect = new Utils.sparkConnection
  val spark = sConnect.connect(tableName)

  case class record(batch: String, rundate: String, table_name: String, error_message: String)

  def main(args: Array[String])={

    val deBug = args.exists{ x => x.toLowerCase.contains("debug=true") }
    val andClause = " and " + args.find(_.startsWith("table_type")).getOrElse("1=1") +
                    " and " + args.find(_.startsWith("table_name")).getOrElse("1=1")

    var isSuccess: Boolean = true
    var errorMessage: String = ""
    var recordSet = new ListBuffer[record]()
    var sqlString = ""
    var validCounter = 0
    var errorCounter = 0

    val batchNumber = spark.sparkContext.applicationId
    val beforeTimestamp: Long = System.currentTimeMillis()
    val formatter = java.text.NumberFormat.getIntegerInstance
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")

    if (deBug) Utils.printLog(deBug,tableName + ": Debug is Enabled for batchNumber: " + batchNumber)

    val cStart = "cast(cast("
    val cEnd = " as BIGINT) as string)"

    try {
          //DataFrame for Table Listing
          sqlString = (f"""
            select table_name, table_type, unique_constraint
            from g00103.iqp_table_listings
            where enabled_flag = 'Y' $andClause%s
            order by table_type, table_name
          """)
          val df_main = spark.sql(sqlString).collect.toList

          //Loop for records in Table Listings
          for (rec <- df_main) {
              //Break the delimited record field values
              val table = rec(0).toString.toLowerCase
              val tType = rec(1).toString
              val dfString = rec(2).toString.toLowerCase
              var constraint = ""

              //Identify the data type and form unique constraint string
              try {
                val df_table = spark.sql(f"select $dfString%s from $table%s")
                val dfArray = df_table.dtypes
                for (i <- 0 to (dfArray.length - 1)) {
                  if (dfArray(i)._2.toLowerCase.contains("double"))
                    constraint += cStart + dfArray(i)._1 + cEnd + ","
                  else
                    constraint += dfArray(i)._1 + ","
                }
              } catch {  //catch Error during unique constriant string forming
                case e: Throwable => {
                  isSuccess = false
                  recordSet += record(batchNumber.toString, dateFormat.format(beforeTimestamp), table, e.getMessage())
                  errorCounter += 1
                  if (deBug) Utils.printLog(deBug, tableName + " Error during constraint build: "  + e.getMessage())
                 }
              }

              if (isSuccess) {   // constraint string is correctly formed
                constraint = constraint.dropRight(1)
                constraint = "concat_ws('~',"  +  constraint + ")"

                //Get the count of the value from Tables
                try {
                    sqlString = (f"""
                      select $constraint%s as value
                            , count(*) as count
                      from $table%s
                      group by $constraint%s
                      having count(*) > 1
                    """)
                    val recCount = spark.sql(sqlString).collect.toList
                    //Record the first value for debugging
                    val recValue = if (recCount.isEmpty) "" else " *Sample Value* " + recCount(0)(0).toString

                  //Evaluate record count results
                  if (recCount.isEmpty) { // This means No Errors, nothing to record
                    val resultStr = "Valid: Table " + table + " for " + constraint
                    validCounter += 1
                    if (deBug) Utils.printLog(deBug, tableName + " : " + resultStr)
                  } else {             //Errors are found, record it in buffer
                    val resultStr = "Error: Table " + table + " of '" + tType + "' for " + constraint + " = " + recValue
                    errorCounter += 1
                    recordSet += record(batchNumber.toString, dateFormat.format(beforeTimestamp), table, resultStr)
                    if (deBug) Utils.printLog(deBug, tableName + " : " + resultStr)
                  }
                } catch { //catch error during finding Record Count
                  case e: Throwable => {
                    isSuccess = false
                    errorCounter += 1
                    recordSet += record(batchNumber.toString, dateFormat.format(beforeTimestamp), table, e.getMessage())
                    if (deBug) Utils.printLog(deBug, tableName + " Error: "  + e.getMessage())
                   }
                }
              } //End if (isSuccess)
                isSuccess = true
          } //End for Loop

          recordSet += record(batchNumber.toString, dateFormat.format(beforeTimestamp), null: String, "Summary: Processed "  +
                             (validCounter + errorCounter) + " records; Valid: " + validCounter + ", Errors: " + errorCounter +
                             "; Optional Filter: " + andClause.replaceAll(" and ","").replaceAll("1=1",""))
          //Convert buffer to dataframe and write to table
          val dfFinal = spark.createDataFrame(recordSet)

          if (deBug) Utils.printLog(deBug,tableName + ": Writing to Table Count: " + formatter.format(dfFinal.count))
          dfFinal.repartition(10).write.format("orc").mode("append").saveAsTable(tableName)
          if (deBug) Utils.printLog(deBug, tableName + ": Processing Completed ...")

    } catch {
      case e: Throwable => {
        isSuccess = false
        errorMessage = e.getMessage()
        if (deBug) Utils.printLog(deBug, tableName + ": Error in Processing: " + errorMessage)
       }
    }
    finally {
      val status = if (isSuccess) "SUCCESS" else "FAILED"
      if (deBug) Utils.printLog(deBug, tableName + ": Writing to audit table with status: " + status)
      Utils.auditRecord(spark, Utils.AuditRecord(batchNumber, tableName, beforeTimestamp, status, errorMessage))
      spark.stop
    }
  } //End of main function

} // End
