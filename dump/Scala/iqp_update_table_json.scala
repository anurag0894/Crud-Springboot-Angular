/*
This program updates table based on onput JSON file information.
Usage:
iqp_update_table_json [debug=true] file=http://HDFScluster/FileName.json

Sample JSIN File
{"table_name":"g00103.pg_invoice_update","batch_number":20,"partition_column":"data_source=19","temporary_partition":"data_source=9999","update_column":"cust_trx_lines_all_description","update_condition":"when order_number is null then 'PG-101'"}
*/

import scala.collection.mutable.ListBuffer
import scala.io.Source

object iqp_update_table_json {
  val sConnect = new Utils.sparkConnection
  val spark = sConnect.connect("iqp_update_table_json")
  val tableName = "IQPUpdateTableJSON"
  var sqlString = ""
  var progressValue = ""

  case class record(colToUpdate: String, colUpdateCondition: String)

  def main(args: Array[String]): Unit = {

    val deBug = args.exists{ x => x.toLowerCase.contains("debug=true") }

    val fileName = args.find(_.toLowerCase.startsWith("filename=")).getOrElse("filename=NONE").split("=")(1)
    if (deBug) Utils.printLog(deBug,tableName + ": JSON FileName: " + fileName)
    if (fileName == "NONE") {
        println("Usage: iqp_update_table_json [debug=true] filename=//hdfsFileName")
        println("Error: FileName not found ....")
        System.exit(1)
    } else {
        try {
            spark.read.json(fileName).createOrReplaceTempView("iqp_update_definitions")
        } catch {
            case e: Throwable => {
              println("Error in File read: " + e.getMessage())
              System.exit(1)
             }
        }
    }

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
                    " from iqp_update_definitions order by table_name, batch_number"
        val df_def = spark.sql(sqlString).collect

        for (rec <- df_def) {
            val sourceTableName = rec(0).toString
            val batchNumber = rec(1).toString
            val sourcePartitionColumn = rec(2).toString
            val withoutPartition = if(sourcePartitionColumn == "") true else false
            val newPartitionColumn = rec(3).toString
            if (deBug) Utils.printLog(deBug,tableName + ": Processing Table: " + sourceTableName +
                 " Batch: " + batchNumber + " Partition: " + sourcePartitionColumn)
            progressValue = "FOR_LOOP_WITH_KEY_VARIABLE_CONFIGURED"

            //Get column and its value from iqp_update_definitions for the table above.
            if (withoutPartition) {
                println("Warning: There is no partition specified, will process entire table ...")
                sqlString = "select lower(update_column) update_column, update_condition from iqp_update_definitions " +
                            "where table_name = '" + sourceTableName + "'" +
                            " and batch_number = " + batchNumber
            }
           else {
               sqlString = "select lower(update_column) update_column, update_condition from iqp_update_definitions " +
                           "where table_name = '" + sourceTableName + "'" +
                           " and batch_number = " + batchNumber +
                           " and partition_column = '" + sourcePartitionColumn +"'"
            }
            val df_cols = spark.sql(sqlString).collect
            recordSet.clear
            columnValues.clear
            for (colBuffer <- df_cols) {
                recordSet += record(colBuffer(0).toString, colBuffer(1).toString)
                columnValues += colBuffer(0).toString
            }
            //Check for duplicate columns in the table
            if (columnValues.size != columnValues.distinct.size) {
                println("Error: Duplicate Columns found in iqp_update_definitions table for table name " + sourceTableName + " for batch number " + batchNumber + " in partition " + sourcePartitionColumn)
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
            if (deBug) Utils.printLog(deBug, tableName + " Temp View Created, Record Count: " + formatter.format(dfNew.count))
            progressValue = "COLUMN_TRANSFORMATION_COMPLETE"

        //Rename selected partition or table from main table
            if (withoutPartition)
                sqlString = "alter table " + sourceTableName + " rename to " + sourceTableName + "_temp"
            else
                sqlString = "alter table " + sourceTableName + " partition (" + sourcePartitionColumn + ") rename to partition (" + newPartitionColumn + ")"

            if (deBug) Utils.printLog(deBug, tableName + " Partition/Table Renamed : " + sourceTableName + " Partition: " + sourcePartitionColumn + " to " + newPartitionColumn)
            //println("Rename Partition/Table: " + sqlString)
            spark.sql(sqlString).collect
            progressValue = "PARTITION_RENAMED"

            spark.sql("set hive.exec.dynamic.partition.mode=nonstrict").collect

        //Insert into new table from temp view with transformed data
            if (withoutPartition)
                sqlString = "create table " + sourceTableName + " as select * from temp"
            else
                sqlString = "insert into " +  sourceTableName + " select * from temp"
            if (deBug) Utils.printLog(deBug, tableName + " Insert into source table: " + sqlString)
            //println("Insert into Target: " + sqlString)
            spark.sql(sqlString).collect
            spark.sql("uncache table temp")
            progressValue = "INSERT_TRANSFORMED_DATA_TO_SOURCE"

        //Drop Temp Partition
            if (withoutPartition)
                sqlString = "drop table " + sourceTableName + "_temp"
            else
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
