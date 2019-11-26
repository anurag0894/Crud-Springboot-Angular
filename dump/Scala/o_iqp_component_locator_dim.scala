import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.DataFrame
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import java.io.File
import org.apache.spark.storage.StorageLevel
import java.text.SimpleDateFormat
import org.apache.spark.sql.functions._


object o_iqp_component_locator_dim
{
  val tableName = "g00103.iqp_component_locator_dim"
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
spark.conf.set("spark.kryo.classesToRegister", "AuditTableSchema")
spark.sql("set spark.sql.shuffle.partitions=10")

val locator_df=spark.sql("""select 
repair_order_number,
component_locator1,
component_locator2,
component_locator3,
component_locator4,
component_locator5,
component_locator6,
component_locator7,
component_locator8,
component_locator9,
component_locator10,
current_timestamp as record_creation_date,
'spark_o_iqp_locator_dim' record_created_by
from g00103.iqp_repairs_dim""")

val locator_array = locator_df.withColumn("component_locator_all" ,array("component_locator1","component_locator2","component_locator3","component_locator4","component_locator5","component_locator6","component_locator7","component_locator8","component_locator9","component_locator10"))
val array_to_string = locator_array.withColumn("concatenated_df", concat_ws(",", locator_array("component_locator_all")))
val exploded_df=array_to_string.withColumn("component_locator_string", explode(split(array_to_string("concatenated_df"), "[,]")))

exploded_df.createOrReplaceTempView("locator_raw") 

spark.sql("""insert overwrite table g00103.IQP_COMPONENT_LOCATOR_DIM  select
repair_order_number,
component_locator_string as component_locator,
record_creation_date,
record_created_by
from 
locator_raw
where 
trim(component_locator_string)!='' and component_locator_string is not null""")

spark.sql("""insert into g00103.IQP_COMPONENT_LOCATOR_DIM select 
iqp_repairs_dim.repair_order_number,
NULL,
current_timestamp,
'spark_o_iqp_locator_dim'
from 
g00103.iqp_repairs_dim
left join g00103.IQP_COMPONENT_LOCATOR_DIM on(iqp_repairs_dim.repair_order_number=IQP_COMPONENT_LOCATOR_DIM.repair_order_number)
where
IQP_COMPONENT_LOCATOR_DIM.repair_order_number is null""")

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
