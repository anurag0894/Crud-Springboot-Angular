

object hiveToCSV
{
  def main(args: Array[String])={

    if (args.length < 2) {
      println("Usage: hiveToCSV tableName /folderPath/  [debug=true]")
      sys.exit(1)
    }

    val tableName = args(0)
    val fileName = args(1) + tableName
    val appName = "hiveToCSV"
    val deBug = args.exists{ x => x.toLowerCase.contains("debug=true") }
    val sConnect = new Utils.sparkConnection
    val spark = sConnect.connect(appName)
    var isSuccess: Boolean = true
    var errorMessage: String = ""

    val batchNumber = spark.sparkContext.applicationId
    val beforeTimestamp: Long = System.currentTimeMillis()
    val formatter = java.text.NumberFormat.getIntegerInstance

    if (deBug) Utils.printLog(deBug,appName + ": Debug is Enabled for batchNumber: " + batchNumber)

    try {
      val df = spark.sql("select * from " + tableName)
      if (deBug) Utils.printLog(deBug,appName + ": Writing to File Count: " + formatter.format(df.count) + " at: " + fileName)
      df.coalesce(1).write
        .option("header","true")
        .option("nullValue","null")
        .option("timestampFormat","yyyy-MM-dd")
        .mode("overwrite")
        .csv(fileName)
      if (deBug) Utils.printLog(deBug, appName + ": Processing Completed ...")

    } catch {
      case e: Throwable => {
        isSuccess = false
        errorMessage = e.getMessage()
        if (deBug) Utils.printLog(deBug, appName + ": Error in Processing: " + errorMessage)
      }
    }
    finally {
      val status = if (isSuccess) "SUCCESS" else "FAILED"
      if (deBug) Utils.printLog(deBug, appName + ": Writing to audit table with status: " + status)
      Utils.auditRecord(spark, Utils.AuditRecord(batchNumber, tableName, beforeTimestamp, status, errorMessage))
      spark.stop
    }
  }
}
