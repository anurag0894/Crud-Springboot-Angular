
object loadViewIntoTable
{
  def main(args: Array[String])={

    if (args.length < 2)  {
      println("Usage: loadViewIntoTable ViewName TableName [debug=true]")
      sys.exit(1)
    }

    val schemaName = "g00103."
    val tableName = schemaName + args(1)
    val viewName = schemaName + args(0)
    val sConnect = new Utils.sparkConnection
    val spark = sConnect.connect(tableName)
    var isSuccess: Boolean = true
    var errorMessage: String = ""

    val deBug = args.exists{ x => x.toLowerCase.contains("debug=true") }

    val batchNumber = spark.sparkContext.applicationId
    val beforeTimestamp: Long = System.currentTimeMillis()
    val formatter = java.text.NumberFormat.getIntegerInstance

    try {
      if (deBug) Utils.printLog(deBug,"Reading all columns from View: " + viewName)
      val sqlStr = "select * from " + viewName
      val df = spark.sql(sqlStr)
      if (deBug) Utils.printLog(deBug,"Writing to target table: " + tableName)
      df.write.format("orc").mode("overwrite").saveAsTable(tableName)
    } catch {
      case e: Throwable => {
        isSuccess = false
        errorMessage = e.getMessage()
        if (deBug) Utils.printLog(deBug,"Error in Processing: " + e.getMessage)
      }
    }
    finally {
      val status = if (isSuccess) "SUCCESS" else "FAILED"
      if (deBug) Utils.printLog(deBug, "Wrting to audit table with status: " + status)
      Utils.auditRecord(spark, Utils.AuditRecord(batchNumber, tableName, beforeTimestamp, status, errorMessage))
      spark.stop
    }
  }
}
