import java.text.SimpleDateFormat

def tblInsert(tblName: String  )  = {
  // Function: Inserting into Internal Hive table + Insert into Audit table

  try {
    val befTime = System.currentTimeMillis()
    val df = spark.sql("select * from " + tblName + "__x").repartition(5)
    df.write.format("orc").mode("overwrite").saveAsTable(tblName)
    //spark.sql("insert overwrite table " + tblName + " select * from " + tblName+"__x" + " distribute by 4" )
    val resultCount =  spark.sql("select count(1) from " + tblName ).as[Long].collect()
    val f = new SimpleDateFormat("yyyy-M-dd hh:mm:ss")
    var afTime=System.currentTimeMillis()
    var timeTaken = afTime - befTime
    val timeelapsed = timeTaken / (1000)
    spark.sql("insert into table g00103.audit_table values('"+ tblName +"',"+ resultCount(0) + ",'" + f.format(befTime) + "','" + f.format(afTime) + "','" + timeelapsed + "','"+ "completed" + "')")
    //df.write.format("orc").mode("overwrite").saveAsTable(tblName)
    //spark.sql("insert overwrite table " + tblName + " select * from " + tblName + "__x" + " distribute by 1")
    //  val resultCount =  spark.sql("select count(1) from " + tblName ).collect()
    // spark.sql("insert into table g00103.audit_table values('"+ tblName +"',"+ resultCount + ",'" + befTime + "','" + System.currentTimeMillis() + "','" + (System.currentTimeMillis() - befTime)+ "','"+ "completed" + "')")
  }
  catch {
    case e: Exception => { }
  }
}

// Main For Loop executed for each record in Master Table, calling function tblInsert
val sourceSchema = "g00103"
val sourceMasterTableName = "gs_iqp_master_table"
val  qualified_tbName = sourceSchema + "." + sourceMasterTableName
val dsTables = spark.sql("select table_name from " + qualified_tbName ).as[String]
dsTables.collect().foreach{tblName:String => {tblInsert(sourceSchema + "."+tblName) }}


/bin/spark-shell  --master yarn --num-executors 7 --driver-memory 4g  --executor-memory 8g --executor-cores 4 --conf spark.scheduler.mode=FAIR  spark.driver.cores=1