package com.bnsf.customreceiver

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{ Seconds, Duration, StreamingContext }
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.dstream._

import org.apache.spark.sql.execution.datasources.hbase._
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.fs.Path
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.hbase.client.ConnectionFactory

import java.security.PrivilegedExceptionAction
import java.security.PrivilegedAction

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructType,StructField,StringType,DoubleType}

/**
 * Counts words in new text files created in the given directory
 * Usage: HdfsWordCount <directory>
 *   <directory> is the directory that Spark Streaming will use to find and read new text files.
 *
 * To run this on your local machine on directory `localdir`, run this example
 *    $ bin/run-example \
 *       org.apache.spark.examples.streaming.HdfsWordCount localdir
 *
 * Then create a text file in `localdir` and the words in the file will get counted.
 */
case class MvgAvgRecord(col0: String, col1 : String, col2 : Double, col3 : Double, col4 : String, col5 : Double, col6 : Double, col7 : String, col8 : Double, col9 : Double, col10 : String, col11 : Double, col12 : Double, col13 : String, col14 : Double, col15 : Double)

object MvgAvgRecord {

	def apply(initial: String, number: String, locationCode: String, locNum: Int, loadEmptyCode: String, col1 : String, col2 : Double, col3 : Double, col4 : String, col5 : Double, col6 : Double, col7 : String, col8 : Double, col9 : Double, col10 : String, col11 : Double, col12 : Double, col13 : String, col14 : Double, col15 : Double) : MvgAvgRecord = {
	  
	  var key =  StringBuilder.newBuilder
	  
	  key.append(initial).append("-").append(number).append("-").append(locationCode).append(locNum).append("-").append(loadEmptyCode)
	  
//      var key = s"""ADLX-      7578-L${"%d".format(i}}-L"""

	  MvgAvgRecord(key.toString(), col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, col11, col12, col13, col14, col15)

	}

}

object JsonTest {

case class PredRecord(col0: String, col1 : String, col2 : Double, col3 : Double, col4 : String, col5 : Double, col6 : Double, col7 : String, col8 : Double, col9 : Double, col10 : String, col11 : Double, col12 : Double, col13 : String, col14 : Double, col15 : Double, col16 : Int)

object PredRecord {

	def apply(initial: String, number: String, locationCode: String, loadEmptyCode: String, col1 : String, col2 : Double, col3 : Double, col4 : String, col5 : Double, col6 : Double, col7 : String, col8 : Double, col9 : Double, 
    col10 : String, col11 : Double, col12 : Double, col13 : String, col14 : Double, col15 : Double, col16 : Int) : PredRecord = {
	  
	  var key =  StringBuilder.newBuilder
	  
	  key.append(initial).append("-").append(number).append("-").append(locationCode).append("-").append(loadEmptyCode)
	  
	  PredRecord(key.toString(), col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, col11, col12, col13, col14, col15, col16)
	}

}
    var mvg_avg_cat = s"""{
           |"table":{"name": "TBL" },
           |"rowkey":"key",
           |"columns":{
           |"col0":{"cf":"rowkey", "col":"key", "type":"string"},
           |"col1":{"cf":"rdgs", "col":"ts1", "type":"string"},
           |"col2":{"cf":"rdgs", "col":"rd1", "type":"double"},
           |"col3":{"cf":"rdgs", "col":"avg1", "type":"double"},
           |"col4":{"cf":"rdgs", "col":"ts2", "type":"string"},
           |"col5":{"cf":"rdgs", "col":"rd2", "type":"double"},
           |"col6":{"cf":"rdgs", "col":"avg2", "type":"double"},
           |"col7":{"cf":"rdgs", "col":"ts3", "type":"string"},
           |"col8":{"cf":"rdgs", "col":"rd3", "type":"double"},
           |"col9":{"cf":"rdgs", "col":"avg3", "type":"double"},
           |"col10":{"cf":"rdgs", "col":"ts4", "type":"string"},
           |"col11":{"cf":"rdgs", "col":"rd4", "type":"double"},
           |"col12":{"cf":"rdgs", "col":"avg4", "type":"double"},
           |"col13":{"cf":"rdgs", "col":"ts5", "type":"string"},
           |"col14":{"cf":"rdgs", "col":"rd5", "type":"double"},
           |"col15":{"cf":"rdgs", "col":"avg5", "type":"double"}
           |}
           |}""".stripMargin

    var pred_cat = s"""{
           |"table":{"name": "TBL" },
           |"rowkey":"key",
           |"columns":{
           |"col0":{"cf":"rowkey", "col":"key", "type":"string"},
           |"col1":{"cf":"rdgs", "col":"ts1", "type":"string"},
           |"col2":{"cf":"rdgs", "col":"rd1", "type":"double"},
           |"col3":{"cf":"rdgs", "col":"avg1", "type":"double"},
           |"col4":{"cf":"rdgs", "col":"ts2", "type":"string"},
           |"col5":{"cf":"rdgs", "col":"rd2", "type":"double"},
           |"col6":{"cf":"rdgs", "col":"avg2", "type":"double"},
           |"col7":{"cf":"rdgs", "col":"ts3", "type":"string"},
           |"col8":{"cf":"rdgs", "col":"rd3", "type":"double"},
           |"col9":{"cf":"rdgs", "col":"avg3", "type":"double"},
           |"col10":{"cf":"rdgs", "col":"ts4", "type":"string"},
           |"col11":{"cf":"rdgs", "col":"rd4", "type":"double"},
           |"col12":{"cf":"rdgs", "col":"avg4", "type":"double"},
           |"col13":{"cf":"rdgs", "col":"ts5", "type":"string"},
           |"col14":{"cf":"rdgs", "col":"rd5", "type":"double"},
           |"col15":{"cf":"rdgs", "col":"avg5", "type":"double"},
           |"col16":{"cf":"pred", "col":"brk", "type":"int"},
           |"col17":{"cf":"trn", "col":"id", "type":"string"}
           |}
           |}""".stripMargin

        val updSchema =  StructType(Array(
                                StructField("rowkey", StringType, true),
                                StructField("ts1", StringType, true), StructField("rd1", DoubleType, true), StructField("avg1", DoubleType, true),
                                StructField("ts2", StringType, true), StructField("rd2", DoubleType, true), StructField("avg2", DoubleType, true),
                                StructField("ts3", StringType, true), StructField("rd3", DoubleType, true), StructField("avg3", DoubleType, true),
                                StructField("ts4", StringType, true), StructField("rd4", DoubleType, true), StructField("avg4", DoubleType, true),
                                StructField("ts5", StringType, true), StructField("rd5", DoubleType, true), StructField("avg5", DoubleType, true)
                                ))
        val predSchema =  StructType(Array(
                                StructField("col0", StringType, true),
                                StructField("col1", StringType, true), StructField("col2", DoubleType, true), StructField("col3", DoubleType, true),
                                StructField("col4", StringType, true), StructField("col5", DoubleType, true), StructField("col6", DoubleType, true),
                                StructField("col7", StringType, true), StructField("col8", DoubleType, true), StructField("col9", DoubleType, true),
                                StructField("col10", StringType, true), StructField("col11", DoubleType, true), StructField("col12", DoubleType, true),
                                StructField("col13", StringType, true), StructField("col14", DoubleType, true), StructField("col15", DoubleType, true),
                                StructField("col16", DoubleType, true)))
                                
  val FAILURE : Int = -1

  def main(args: Array[String]) {

    if (args.length < 1) {
      System.err.println("Usage: HdfsWordCount <directory>")
      System.exit(1)
    }

    val sparkConf = new SparkConf().setAppName("HdfsWordCount").set("hbase.security.authentication", "kerberos").set("hbase.security.authorization", "true")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    // Create the context
    // val ssc = new StreamingContext(sparkConf, Seconds(2))
    // val ssc = new StreamingContext(sc, Seconds(2))

    println("DBG: Past sparkSqlContext init ..")
    import sqlContext.implicits._

        def withCatalog(cat: String): DataFrame = {
          sqlContext
            .read
            .options(Map(HBaseTableCatalog.tableCatalog -> cat))
            .format("org.apache.spark.sql.execution.datasources.hbase")
            .load()
        }

    // Create the FileInputDStream on the directory and use the
    // stream to count words in new files created
    // val lines = ssc.textFileStream(args(0))
    val rdd = sc.textFile(args(0))

        val mvgAvgTbl = "techm:mvg_avg"
        mvg_avg_cat = mvg_avg_cat.replaceFirst("TBL", mvgAvgTbl)
        println("DBG AFT REPLACE tbl=" + mvg_avg_cat)
/*
    val data = (1  to  6).map { i =>
    //val s = s"""row${"%03d".format(i)}"""
     MvgAvgRecord("BNSF", "      3802", "L", i, "U", "2017-03-05 17:50:00", 5.0, 4.0, "2017-03-04 17:50:00", 4.0, 3.0, "2017-03-03 17:50:00", 3.0, 2.0, "2017-03-02 17:50:00", 2.0, -99999.0, "2017-03-01 17:50:00", 1.0, -99999.0)

    }

    sc.parallelize(data).toDF.write.options(
      Map(HBaseTableCatalog.tableCatalog -> mvg_avg_cat, HBaseTableCatalog.newTable -> "2"))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()
    println("DBG: post Write ..")
*/
        val mvgAvgDF = withCatalog(mvg_avg_cat)
        mvgAvgDF.registerTempTable("mvgAvgTbl")
        println("DBG MVG_AVG Count =" + mvgAvgDF.count())

        val tstDF = sqlContext.sql("select * from mvgAvgTbl ")
        tstDF.collect.foreach(println)
        tstDF.printSchema

        println("DBG predG BEF withCat =" + pred_cat)
        val predTbl = "techm:whl_brk_pred"
        pred_cat = pred_cat.replaceFirst("TBL", predTbl)
        println("DBG AFT REPLACE tbl=" + pred_cat)

        val predDF = withCatalog(pred_cat)
        predDF.registerTempTable("predTbl")
        println("DBG predG Count =" + predDF.count())
        predDF.show() //.foreach(println)

    // lines.foreachRDD ( rdd => {

      if( ! rdd.isEmpty) {

        val jsonMsgsDF = sqlContext.read.json(rdd)
        jsonMsgsDF.registerTempTable("jsonMsgs")
        val cntDF = sqlContext.sql(" select count(1) from jsonMsgs")
      
        println("DBG jsonMsgTbl count ")
        cntDF.collect.foreach(println)
      
       // val jsonWhlMeasDF = sqlContext.sql("select initial, number, loadEmptyCode, locationCode as locationCode, whlMeas.timestamp as timestamp, whlMeas.strValue as strValue, whlMeas.typeCode as typeCode from (select initial, number,  loadEmptyCode, whl.locationCode as locationCode, whl.measurements as measurements from ( select eqp.initial as initial, eqp.number as number , eqp.loadEmptyCode as loadEmptyCode,   eqp.wheels as wheels from jsonMsgsTbl LATERAL VIEW explode(body.train.equipments) t1 as eqp ) tab1 LATERAL VIEW explode(tab1.wheels) t2 as whl  ) tab2 LATERAL VIEW explode(measurements)  t3 as whlMeas where whlMeas.typeCode = 'whl_kips_dyn'")

        val jsonWhlMeasDF = sqlContext.sql("select trn_id, initial, number, loadEmptyCode, locationCode as locationCode, " +
          " whlMeas.strValue as strValue, whlMeas.typeCode as typeCode, whlMeas.timestamp as timestamp from ( " +
          "   select trn_id, initial, number,  loadEmptyCode, whl.locationCode as locationCode, " +
          "          whl.measurements as measurements from (select eqp.initial as initial, " +
          "          eqp.number as number , eqp.loadEmptyCode as loadEmptyCode, body.train.trainId as trn_id, eqp.wheels as wheels " +
          "       from jsonMsgs LATERAL VIEW explode(body.train.equipments) t1 as eqp where header.type = 'WILD' " +
          " ) tab1 LATERAL VIEW explode(tab1.wheels) t2 as whl  " +
          " ) tab2 LATERAL VIEW explode(measurements)  t3 as whlMeas " + 
          " where whlMeas.typeCode = 'whl_kips_dyn'")

        System.out.println(System.currentTimeMillis() + "::DBG::WildErrMsgProcessor::processAlgo Able to print records START")
        jsonWhlMeasDF.collect.foreach(println)
        System.out.println(System.currentTimeMillis() + "::DBG::WildErrMsgProcessor::processAlgo Able to print records END")
      
        // val whlMeasDF = sqlContext.sql(" select eqp.initial as initial, eqp.number as number , eqp.loadEmptyCode as loadEmptyCode,   eqp.wheels as wheels from jsonMsgsTbl LATERAL VIEW explode(body.train.equipments) t1 as eqp ")

        println("DBG EXPLODE jsonMsgTbl count ")
        jsonWhlMeasDF.collect.foreach(println)
        jsonWhlMeasDF.registerTempTable("jsonWhlMeasTbl")

        //Join with mvg_avg table
        // val joinAvgNewRdg = sqlContext.sql("select mvg.col0, mvg.col1, rdg.timestamp, rdg.strValue from jsonWhlMeasTbl rdg left outer join mvgAvgTbl mvg on ( concat(initial, '-', number,'-', locationCode, '-', loadEmptyCode) = mvg.col0 )  ")

        val joinAvgNewRdg = sqlContext.sql(" select  rdg.initial, rdg.number, rdg.locationCode, rdg.loadEmptyCode, " + 
	  " rdg.timestamp as col1, cast(rdg.strValue as double) as col2, (cast(rdg.strValue as double) + mvg.col2 + mvg.col5)/3 as col3, " + 
	  " mvg.col1 as col4, coalesce(mvg.col2, -99999.0) as col5, mvg.col3 as col6, " +
	  " mvg.col4 as col7, coalesce(mvg.col5, -99999.0) as col8, mvg.col6 as col9, " +
	  " mvg.col7 as col10, coalesce(mvg.col8, -99999.0) as col11, mvg.col9 as col12, " + 
	  " mvg.col10 as col13, coalesce(mvg.col11, -99999.0) as col14, mvg.col12 as col15, " + 
	  " case when mvg.col2 <> -99999.0 and mvg.col5 <> -99999.0 and " +
          " 	(( (cast(rdg.strValue as double) + mvg.col2 + mvg.col5)/3 ) >= mvg.col6 and mvg.col6 >= mvg.col9 ) and " +
          " 		(( (cast(rdg.strValue as double) + mvg.col2 + mvg.col5)/3 ) - mvg.col6 >= 5.0 ) and " +
          " 		(datediff(rdg.timestamp, mvg.col4) <= 60 ) " +
          " 		then 1 else 0 end as col16 " +
          " from jsonWhlMeasTbl rdg join mvgAvgTbl mvg on (concat(rdg.initial,'-',rdg.number,'-',rdg.locationCode, '-', rdg.loadEmptyCode) = mvg.col) ")

		println("DBG JOIN jsonMsgTbl count " + joinAvgNewRdg.count())
		println("DBG JOIN jsonMsgTbl records ")
		joinAvgNewRdg.collect.foreach(println)
      }

	    // } )

	/*    
	    val words = lines.flatMap(_.split(" "))
	    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
	    wordCounts.print()
	*/
	    // ssc.start()
	    // ssc.awaitTermination()
	    
	  }
	}
	// scalastyle:on println
