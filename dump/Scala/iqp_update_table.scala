/*
This update works only on pratitioned table
At any time provide the rows in the partition that needs update, for which column on what condition and with what new value.

create table g00103.iqp_update_definitions
(
table_name string,
batch_number int,
partition_column string,
update_column string,
case_when string,
temporary_partition string,
enabled_flag varchar(1)
)
STORED AS ORC;

INSERT INTO g00103.iqp_update_definitions VALUES
('g00103.pg_invoice_update',10,'data_source=19','BILL_TO_CUSTOMER_NUMBER',"WHEN UPPER(TRIM(ORDER_NUMBER)) IN ('S0013895') AND UPPER(TRIM(PRODUCT_Number)) IN ('1929A20835-D') AND UPPER(TRIM(BILL_TO_CUSTOMER_NUMBER)) ='611890T' THEN '4594K06'",'data_source=-99999','Y')
,('g00103.pg_invoice_update',10,'data_source=19','BILL_TO_CUSTOMER_NAME',"WHEN UPPER(TRIM(ORDER_NUMBER)) IN ('S0013895') AND UPPER(TRIM(PRODUCT_Number)) IN ('1929A20835-D') AND UPPER(TRIM(BILL_TO_CUSTOMER_NUMBER)) ='611890T' THEN 'MAGNA ELECTRIC CORPORATION'",'data_source=-99999','Y'),
('g00103.pg_invoice_update',20,'data_source=19','BILL_TO_CUSTOMER_NUMBER',"when UPPER(TRIM(ORDER_NUMBER)) IN ('S0273006') AND UPPER(TRIM(PRODUCT_Number)) IN ('786-601MR') then '1001' when UPPER(TRIM(ORDER_NUMBER)) IN ('S0273006') AND UPPER(TRIM(PRODUCT_Number)) IN ('786-601MR') AND UPPER(TRIM(BILL_TO_CUSTOMER_NUMBER))='611890T' THEN '6089B01'",'data_source=-99999','Y')
,('g00103.pg_invoice_update',20,'data_source=19','BILL_TO_CUSTOMER_NAME',"when UPPER(TRIM(ORDER_NUMBER)) IN ('S0273006') AND UPPER(TRIM(PRODUCT_Number)) IN ('786-601MR') then 'PANKAJ GUPTA' when UPPER(TRIM(ORDER_NUMBER)) IN ('S0273006') AND UPPER(TRIM(PRODUCT_Number)) IN ('786-601MR') AND UPPER(TRIM(BILL_TO_CUSTOMER_NUMBER))='611890T' THEN 'ATLANTIC TRACTORS & EQUI'",'data_source=-99999','Y')

Usage:
iqp_update_table [debug=true] [table_name=SpecificTableToUpdate] [batch_number=SpecificBatchWithinSpecificTable]

*/

import scala.collection.mutable.ListBuffer

object iqp_update_table {
  val sConnect = new Utils.sparkConnection
  val spark = sConnect.connect("iqp_update_table")
  val tableName = "IQPUpdateTable"
  var sqlString = ""
  var progressValue = ""

  case class record(colToUpdate: String, colUpdateCondition: String)

  def main(args: Array[String]): Unit = {

    val deBug = args.exists{ x => x.toLowerCase.contains("debug=true") }
    val andClause = " and " + args.find(_.startsWith("table_name=")).getOrElse("1=1") +
                    " and " + args.find(_.startsWith("batch_number=")).getOrElse("1=1")

    var isSuccess: Boolean = true
    var errorMessage: String = ""

    val batchNumber = spark.sparkContext.applicationId
    val beforeTimestamp: Long = System.currentTimeMillis()
    val formatter = java.text.NumberFormat.getIntegerInstance
    var recordSet = new ListBuffer[record]()
    var columnValues = new  ListBuffer[String]()

    if (deBug) Utils.printLog(deBug,tableName + ": Debug is Enabled for batchNumber: " + batchNumber)
    try {
        //Get Data from the iqp_update_definitions
        sqlString = "select distinct table_name, batch_number, partition_column, temporary_partition " +
                    " from g00103.iqp_update_definitions  " +
                    " where enabled_flag = 'Y' " + andClause +
                    " order by table_name, batch_number"
        val df_def = spark.sql(sqlString).collect

        for (rec <- df_def) {
            val sourceTableName = rec(0).toString
            val batchNumber = rec(1).toString
            val sourcePartitionColumn = rec(2).toString
            val newPartitionColumn = rec(3).toString
            if (deBug) Utils.printLog(deBug,tableName + ": Processing Table: " + sourceTableName +
                 " Batch: " + batchNumber + " Parition: " + sourcePartitionColumn)
            progressValue = "FOR_LOOP_WITH_KEY_VARIABLE_CONFIGURED"

            //Get column and its value from iqp_update_definitions for the table above.
            sqlString = "select lower(update_column) update_column, case_when from g00103.iqp_update_definitions " +
                        "where table_name = '" + sourceTableName + "'" +
                        " and batch_number = " + batchNumber +
                        " and enabled_flag = 'Y' " +
                        " and partition_column = '" + sourcePartitionColumn +"'"
            val df_cols = spark.sql(sqlString).collect
            recordSet.clear
            columnValues.clear
            for (colBuffer <- df_cols) {
                recordSet += record(colBuffer(0).toString, colBuffer(1).toString)
                columnValues += colBuffer(0).toString
            }
            //Check for duplicate columns in the table
            if (columnValues.size != columnValues.distinct.size) {
                println("Error: Duplicate Columns found in g00103.iqp_update_definitions table for table name " + sourceTableName + " for batch number " + batchNumber + " in partition " + sourcePartitionColumn)
                System.exit(1)
            }
            progressValue = "FOUND_ALL_SOURCE_TABLE_COLUMNS"
            //DataFrame to get columns from source table
            val df = spark.sql("select * from " + sourceTableName)
            val cols = df.columns

            //Build the transformed String
            sqlString = ""
            var counter = 0
            for (col <- cols) {
                counter += 1
                val colSet = recordSet.filter(_.colToUpdate.equals(col))
                if (colSet.isEmpty) //Column not foud in ListBuffer
                   sqlString += col + ", "
                else
                   sqlString += " case " + colSet(0).colUpdateCondition + " else " + colSet(0).colToUpdate + " end as " + colSet(0).colToUpdate + ", "
            }
            if (deBug) Utils.printLog(deBug,tableName + ": Column Count in Temp View: " + counter)
            sqlString = sqlString.substring(0,sqlString.length - 2)
            //Create temp view from source Table
            sqlString = "select " + sqlString + " from " + sourceTableName + " where " +  sourcePartitionColumn
            val dfNew = spark.sql(sqlString)
            //println("Temp View: " + sqlString)
            dfNew.collect
            dfNew.createOrReplaceTempView("temp")
            spark.sql("cache table temp")
            if (deBug) Utils.printLog(deBug, tableName + " Temp View Created: " + formatter.format(dfNew.count))
            progressValue = "COLUMN_TRANSFORMATION_COMPLETE"

            //Rename selected partition from main table
            sqlString = "alter table " + sourceTableName + " partition (" + sourcePartitionColumn + ") rename to partition (" + newPartitionColumn + ")"
            if (deBug) Utils.printLog(deBug, tableName + " Partition Renamed : " + sourceTableName + " Partition: " + sourcePartitionColumn + " to " + newPartitionColumn)
            //println("Rename Partition: " + sqlString)
            spark.sql(sqlString).collect
            progressValue = "PARTITION_RENAMED"

            spark.sql("set hive.exec.dynamic.partition.mode=nonstrict").collect

            //insert into new table from temp view with transformed data
            sqlString = "insert into " +  sourceTableName + " select * from temp"
            if (deBug) Utils.printLog(deBug, tableName + " Insert into source table: " + sqlString)
            //println("Insert into Target: " + sqlString)
            spark.sql(sqlString).collect
            spark.sql("uncache table temp")
            progressValue = "INSERT_TRANSFORMED_DATA_TO_SOURCE"

            //Drop Temp Partition
            sqlString = "alter table " + sourceTableName + " drop partition (" + newPartitionColumn + ")"
            if (deBug) Utils.printLog(deBug, tableName + " Partition Dropped : " + sourceTableName + " Partition: " + newPartitionColumn)
            //println("Drop Partition: " + sqlString)
            spark.sql(sqlString).collect
            progressValue = "DROP_TEMP_PARTITION"
        }
    } catch {
          case e: Throwable => {
            isSuccess = false
            errorMessage = e.getMessage()
            if (deBug) Utils.printLog(deBug, tableName + ": Error in Processing: " + errorMessage +
                 "\n Last SQL: " + sqlString + "\n progressValue: " + progressValue)
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
