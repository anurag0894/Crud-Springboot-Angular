package com.bnsf.customreceiver

import com.bnsf.model.ioc._
import com.bnsf.model.ioc.Constants
//import com.bnsf.model.ioc.Message
//import com.bnsf.model.ioc.Header

import java.util.Hashtable
//remove if not needed
import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

import util.control.Breaks._

import com.fasterxml.jackson.core.JsonGenerationException
import com.fasterxml.jackson.databind.{ DeserializationFeature, ObjectMapper }
import com.fasterxml.jackson.databind.JsonMappingException
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{ Seconds, Duration, StreamingContext }
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.dstream._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructType,StructField,StringType,DoubleType}

import javax.jms._
import javax.naming.Context
import javax.naming.InitialContext
import javax.naming.NamingException

import java.io.File
import java.nio.charset.Charset

import java.util.Calendar
import java.text.SimpleDateFormat

//import com.google.common.io.Files
import java.io.BufferedReader
import java.io.InputStreamReader
import java.io.InputStream
import java.io.IOException

import java.net.ConnectException
import java.net.Socket
import java.nio.charset.StandardCharsets
import java.util.regex.Pattern

import java.text.SimpleDateFormat

import java.util.Date

import javax.jms._

import org.apache.spark.sql.execution.datasources.hbase._
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.hbase.client.ConnectionFactory

import java.security.PrivilegedExceptionAction
import java.security.PrivilegedAction

// import scala.util.Properties
import java.util.{ Hashtable, Properties }

import com.tibco.tibjms.Tibjms._
// import javax.jms.Session.CLIENT_ACKNOWLEDGE

object WildMessageReceiverHDP {

  private val SPACE = Pattern.compile(" ")

  val DELIMITER = '\001'
  def getDateInHH(): String = {

    val now = Calendar.getInstance().getTime()
    val minuteFormat = new SimpleDateFormat("yyyyMMdd-HH")
    val currDateHr = minuteFormat.format(now)
    currDateHr
  }

  def buildMsgList(iterator: Iterator[String]): Iterator[Detector_Message] = {

    // mapper object created on each executor node
    val mapper = new ObjectMapper() // with ScalaObjectMapper
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    mapper.registerModule(DefaultScalaModule)

    val records = iterator.toList
    val result = records.map(record => {

      try {

        val message: JsonNode = mapper.readTree(record).path("header")
        // if (message != null){

        //From Header get MsgId and Detector type
        val msgId: Long = message.path("messageId").asLong()

        val msgType: String = message.path("type").asText()

        //Detector_Message(msgId + "-" + msgType , msgId, msgType, record)

        // case class Detector_Message ( rowkey: String, detr_typ: String, msg_ver: String, msg_id: Long, trn_id: String, trn_ps_ts: String, detr_id: String, msg : String, stat_cd: String, cret_ts: Long)
        Detector_Message(msgId + "-" + msgType, msgType, "msg_ver", msgId, "trn_id", "trn_ps_ts", "detr_id", record, "U", System.currentTimeMillis())

        // } else
        //   println(System.currentTimeMillis() + " DBG in buildMsgList message object is NULL ")

      } catch {
        case ex: Exception => {
          println("DBG in buildMsgList Caught Exception")
          ex.printStackTrace()
          throw ex
        }
      }
    }).iterator //End map

    result

  }

  def buildMsgTuple(iterator: Iterator[String]): Iterator[String] = {

    // mapper object created on each executor node
    val mapper = new ObjectMapper() // with ScalaObjectMapper
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    mapper.registerModule(DefaultScalaModule)

    val records = iterator.toList
    val result = records.map(record => {

      try {

        val message: JsonNode = mapper.readTree(record).path("header")
        // if (message != null){

        //From Header get MsgId and Detector type
        val msgId: Long = message.path("messageId").asLong()

        val msgType: String = message.path("type").asText()

        val line = StringBuilder.newBuilder
        // Detector_Message( msgId + "-" + msgType , msgType, "msg_ver", msgId, "trn_id", "trn_ps_ts", 0L , record, "U", System.currentTimeMillis() )
        line.append(msgId + "-" + msgType).append(DELIMITER)
          .append(msgType).append(DELIMITER)
          .append("msg_ver").append(DELIMITER)
          .append(msgId).append(DELIMITER)
          .append("trn_id").append(DELIMITER)
          .append("trn_ps_ts").append(DELIMITER)
          .append(0L).append(DELIMITER)
          .append(record).append(DELIMITER)
          .append("U").append(DELIMITER)
          .append(System.currentTimeMillis)
        line.toString
        // } else
        //    println(System.currentTimeMillis() + " DBG in buildMsgList message object is NULL ")

      } catch {
        case ex: Exception => {
          println("DBG in buildMsgTuple Caught Exception")
          ex.printStackTrace()
          throw ex
        }
      }
    }).iterator //End map

    result

  }

  def WriteToHDFS(msgDstream: ReceiverInputDStream[String], hdfsPath: String): Boolean = {

    var hdfsWrite : Boolean = false
    println("DBG: in WriteToHDFS")

    try {
      val currDateHr = getDateInHH()

      val result = msgDstream.mapPartitions(buildMsgTuple)	//.repartition(1)
      result.foreachRDD { rdd =>
        rdd.saveAsTextFile(hdfsPath + "/cret_ts=" + System.currentTimeMillis() + "/")
      }

      hdfsWrite = true
    } catch {
        case ex: Exception => {
          println("DBG::WriteToHDFS:: Exception writing to HDFS, will try to stop the application ")
          ex.printStackTrace()
          throw ex
        }
    }

    hdfsWrite

  }

  def WriteToHBase(msgRDD: ReceiverInputDStream[String], hbaseTbl: String): Boolean = {

    println("DBG: in WriteToHBase")

    var hbaseWrite : Boolean = false

    try {

      msgRDD.foreachRDD { rdd =>
  
        // Get the singleton instance of SQLContext
        val sqlContext = SQLContext.getOrCreate(rdd.sparkContext)
        import sqlContext.implicits._
  
        val tbl = """${"%s".format(hbaseTbl)}"""
        // case class Detector_Message ( rowkey: String, detr_typ: String, msg_ver: String, msg_id: Long, trn_id: String, trn_ps_ts: String, detr_id: String, msg : String, stat_cd: String, cret_ts: Long)
  
        var cat = s"""{
                     |"table":{"name": "TBL" },
                     |"rowkey":"key",
                     |"columns":{
                     |"col0":{"cf":"rowkey", "col":"key", "type":"string"},
                     |"col1":{"cf":"hdr", "col":"detr_typ", "type":"string"},
                     |"col2":{"cf":"hdr", "col":"msg_ver", "type":"string"},
                     |"col3":{"cf":"hdr", "col":"msg_id", "type":"long"},
                     |"col4":{"cf":"hdr", "col":"trn_id", "type":"string"},
                     |"col5":{"cf":"hdr", "col":"trn_ps_ts", "type":"string"},
                     |"col6":{"cf":"hdr", "col":"detr_id", "type":"string"},
                     |"col7":{"cf":"dtl", "col":"msg", "type":"string"},
                     |"col8":{"cf":"status", "col":"stat_cd", "type":"string"},
                     |"col9":{"cf":"status", "col":"cret_ts", "type":"long"}
                     |}
                     |}""".stripMargin
  
        // println("DBG BEF REPLACE tbl=" + hbaseTbl + "#tbl=" + tbl + "#cat=" + cat)
        cat = cat.replaceFirst("TBL", hbaseTbl)
        // println("DB AFT REPLACE tbl=" + hbaseTbl + "#tbl=" + tbl + "#cat=" + cat)
  
        val result = rdd.mapPartitions(buildMsgList)
  
        println("DBG: before parallelize write .." + result.getNumPartitions)
  
        // for testing connection sharing only
        result.toDF.write.options(
          Map(HBaseTableCatalog.tableCatalog -> cat, HBaseTableCatalog.newTable -> "3"))
          .format("org.apache.spark.sql.execution.datasources.hbase")
          .save()
  
      }
      hbaseWrite = true

    } catch {
        case ex: Exception => {
          println("DBG::WriteToHBase:: Exception writing to HBase")
          ex.printStackTrace()
          throw ex
        }
    }
    println("DBG: post HBase Write ..")
    hbaseWrite 

  }
  
  def processAlgo(msgDStream: ReceiverInputDStream[String], mvgAvgTbl: String, predTbl: String): Unit = {

    println("DBG: in WriteToprocessAlgo")

    val currDateHr = getDateInHH()
    val tbl = """${"%s".format(mvgAvgTbl)}"""

    val result = msgDStream.transform ( rdd => { 
        // Get the singleton instance of SQLContext
        val sqlContext = SQLContext.getOrCreate(rdd.sparkContext)
        import sqlContext.implicits._

        val jsonMsgs = sqlContext.read.json(rdd)
        jsonMsgs.registerTempTable("jsonMsgs")
        
        val whlMeasDF = sqlContext.sql("select initial, number, loadEmptyCode, locationCode as locationCode, " +  
                          " whlMeas.strValue as strValue, whlMeas.typeCode as typeCode from ( " + 
                          "   select initial, number,  loadEmptyCode, whl.locationCode as locationCode, " + 
                          "          whl.measurements as measurements from (select eqp.initial as initial, " + 
                          "          eqp.number as number , eqp.loadEmptyCode as loadEmptyCode,   eqp.wheels as wheels " + 
                          "       from jsonMsgs LATERAL VIEW explode(body.train.equipments) t1 as eqp " + 
                          " ) tab1 LATERAL VIEW explode(tab1.wheels) t2 as whl  " + 
                          " ) tab2 LATERAL VIEW explode(measurements)  t3 as whlMeas " + 
                          " where whlMeas.typeCode = 'whl_kips_dyn'")
        //  whlMeasDF.map { x => x }                
        println("In processAlgo..Able to print records START")
        whlMeasDF.collect.foreach(println)
        println("In processAlgo..Able to print records END")
        whlMeasDF.map { x => x }
    } )
 
   /*
    msgDStream.foreachRDD ( rdd => {

        // Get the singleton instance of SQLContext
        val sqlContext = SQLContext.getOrCreate(rdd.sparkContext)
        import sqlContext.implicits._

        val jsonMsgs = sqlContext.read.json(rdd)
        jsonMsgs.registerTempTable("jsonMsgs")
        
        val whlMeasDF = sqlContext.sql("select initial, number, loadEmptyCode, locationCode as locationCode, " +  
                          " whlMeas.strValue as strValue, whlMeas.typeCode as typeCode from ( " + 
                          "   select initial, number,  loadEmptyCode, whl.locationCode as locationCode, " + 
                          "          whl.measurements as measurements from (select eqp.initial as initial, " + 
                          "          eqp.number as number , eqp.loadEmptyCode as loadEmptyCode,   eqp.wheels as wheels " + 
                          "       from jsonMsgs LATERAL VIEW explode(body.train.equipments) t1 as eqp " + 
                          " ) tab1 LATERAL VIEW explode(tab1.wheels) t2 as whl  " + 
                          " ) tab2 LATERAL VIEW explode(measurements)  t3 as whlMeas " + 
                          " where whlMeas.typeCode = 'whl_kips_dyn'")
                          
        println("In processAlgo..Able to print records START")
        whlMeasDF.collect.foreach(println)
        println("In processAlgo..Able to print records END")
    })
    */
}
    /*
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
           |"col15":{"cf":"rdgs", "col":"avg5", "type":"double"}
           |"col16":{"cf":"pred", "col":"brk", "type":"int"}
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
                                
        def withCatalog(cat: String): DataFrame = {
          sqlContext
            .read
            .options(Map(HBaseTableCatalog.tableCatalog -> cat))
            .format("org.apache.spark.sql.execution.datasources.hbase")
            .load()
        }

        // mapper object created on each executor node
        val mapper = new ObjectMapper() // with ScalaObjectMapper
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        mapper.registerModule(DefaultScalaModule)

        // println("DBG BEF REPLACE tbl=" + hbaseTbl + "#tbl=" + tbl + "#cat=" + cat)
        mvg_avg_cat = mvg_avg_cat.replaceFirst("TBL", mvgAvgTbl)
        pred_cat = pred_cat.replaceFirst("TBL", predTbl)

        rdd.foreach(record => {

            var body: JsonNode = mapper.readTree(record).path("body")

            var bodyNode: JsonNode = body.get(Constants.BODY)
            var detectorNode: JsonNode = bodyNode.get(Constants.DECTECTOR)
            var decTypeCode: String = detectorNode.get(Constants.DEC_TYPE_CODE).asText()

            if (decTypeCode.toLowerCase() == Constants.DEC_TYPE_CODE_WILD.toLowerCase()) {

              var trainNode: JsonNode = bodyNode.get("train")

              var equipNodeArr: JsonNode = trainNode.get("equipments")

              for (equipNode: JsonNode <- equipNodeArr) {

                var wheelArr: JsonNode = equipNode.get("wheels")
                for (wheelNode: JsonNode <- wheelArr) {

                  var mrsArr: JsonNode = wheelNode.get("measurements")
                  for (mrs: JsonNode <- mrsArr) {
                    println("DBG: in Wheel meas..")

                    if (mrs.get("typeCode").asText().toLowerCase() == Constants.WHL_KIPS_DYN.toLowerCase()) {

                      var eqpInit: String = equipNode.get("initial").asText()
                      var eqpNumb: String = equipNode.get("number").asText()
                      var locCode: String = wheelNode.get("locationCode").asText()
                      var loadMty: String = equipNode.get("loadEmptyCode").asText()
                      var timeStamp: String = mrs.get("timestamp").asText()
                      var wheelKipsDyn: Double = mrs.get("strValue").asText().trim().toDouble

                      println("DBG: in Wheel meas.." + eqpInit + "#" + eqpInit + "#" + locCode + "#" + loadMty + "#" + timeStamp + "#" + wheelKipsDyn)

                      //								 wildDetectorBean.setInitial(initial)
                      //								 wildDetectorBean.setNumber(number)
                      //								 wildDetectorBean.setLocationCode(locationCode)
                      //								 wildDetectorBean.setCarId(initial+number)
                      //								 wildDetectorBean.setWheelId(initial+number+locationCode)
                      //								 wildDetectorBean.setLoadMty(loadMty)
                      //								 wildDetectorBean.setTimestamp(timestamp)
                      //								 wildDetectorBean.setWheelKipsDyn(Double.parseDouble(wheelKipsDyn))
                      //								 wildDetectorBeans.add(wildDetectorBean)

                      //								 detectDefectWhls(wildDetectorBean)

                      // val rowkey = eqpInit + "-" + eqpNumb + "-" + locCode + "-" + loadMty
                      val rowkey = "WFAX-      6016-R4-U"

                      val df = withCatalog(mvg_avg_cat)
                      df.registerTempTable("mvg_avg")

                      var result = sqlContext.sql("select * from mvg_avg where col0 = '" + rowkey + "'")	//.repartition(1)

                      result.cache()
                      
                      case class BrokenWhl(key: String,
                                ts1: String, rd1: Double, avg1: Double, //ts1 is Most recent readings to ts5 is least reading
                                ts2: String, rd2: Double, avg2: Double,
                                ts3: String, rd3: Double, avg3: Double,
                                ts4: String, rd4: Double, avg4: Double,
                                ts5: String, rd5: Double, avg5: Double,
                                brk: Double)
                                
                      val predRowRDD = result.map{
                        
                        row =>
                          {

                            val brk: Int = detectDefectWhls(row, wheelKipsDyn, timeStamp)

                            row match {

                              case Row(key: String,
                                ts1: String, rd1: Double, avg1: Double, //ts1 is Most recent readings to ts5 is least reading
                                ts2: String, rd2: Double, avg2: Double,
                                ts3: String, rd3: Double, avg3: Double,
                                ts4: String, rd4: Double, avg4: Double,
                                ts5: String, rd5: Double, avg5: Double) => {
                                  
                                  var newAvg = (wheelKipsDyn + rd1 + rd2)/3.0
                                  
                                  println("DBG in calc loop#" + key + "#" + timeStamp + "#" + wheelKipsDyn + "#" + newAvg + "#" + ts1 + "#" + rd1 + "#" + avg1 + "#" + ts2 + "#" + rd2 + "#" + avg2 + "#" + ts3 + "#" + rd3 + "#" + avg3 + "#" + ts4 + "#" + rd4 + "#" + avg4)
                                  Row(key, timeStamp, wheelKipsDyn, newAvg, ts2, rd2, avg2, ts3, rd3, avg3, ts4, rd4, avg4, ts5, rd5, avg5, brk)
                                  
                              }
                            }
                          }
                      }

                      println("DBG: after getRecord.." + result.rdd.getNumPartitions)
                      sqlContext.createDataFrame(predRowRDD, predSchema).write
                                .options(Map(HBaseTableCatalog.tableCatalog -> pred_cat, HBaseTableCatalog.newTable -> "1"))
                                .format("org.apache.spark.sql.execution.datasources.hbase")
                                .save()

                      val updRowRDD = result.map {
                        
                              case Row(key: String,
                                ts1: String, rd1: Double, avg1: Double, //ts1 is Most recent readings to ts5 is least reading
                                ts2: String, rd2: Double, avg2: Double,
                                ts3: String, rd3: Double, avg3: Double,
                                ts4: String, rd4: Double, avg4: Double,
                                ts5: String, rd5: Double, avg5: Double) => {
                                  
                                  var newAvg = (wheelKipsDyn + rd1 + rd2)/3.0
                                  println("DBG in upd loop#" + key + "#" + timeStamp + "#" + wheelKipsDyn + "#" + newAvg + "#" + ts1 + "#" + rd1 + "#" + avg1 + "#" + ts2 + "#" + rd2 + "#" + avg2 + "#" + ts3 + "#" + rd3 + "#" + avg3 + "#" + ts4 + "#" + rd4 + "#" + avg4)
                                  Row(key, timeStamp, wheelKipsDyn, newAvg, ts1, rd1, avg1, ts2, rd2, avg2, ts3, rd3, avg3, ts4, rd4, avg4)
                                  
                              }
                      }
                      
                      sqlContext.createDataFrame(updRowRDD, updSchema).write
                                .options(Map(HBaseTableCatalog.tableCatalog -> mvg_avg_cat, HBaseTableCatalog.newTable -> "1"))
                                .format("org.apache.spark.sql.execution.datasources.hbase")
                                .save()

                    }//If wheel kips dyn

                  }//Wheel Meas

                }//Wheel

              }//Eqp

            }//WILD

        }) //map record in Parn


      })//foreachRDD

    println("DBG: post processAlgo..")

  }
*/
    
  def processAlgo1(msgDStream: ReceiverInputDStream[String], mvgAvgTbl: String, predTbl: String): Unit = {

    println("DBG: in WriteToprocessAlgo")

    val currDateHr = getDateInHH()
    val tbl = """${"%s".format(mvgAvgTbl)}"""

    msgDStream.foreachRDD ( rdd => {

        // Get the singleton instance of SQLContext
        val sqlContext = SQLContext.getOrCreate(rdd.sparkContext)
        import sqlContext.implicits._

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
           |"col15":{"cf":"rdgs", "col":"avg5", "type":"double"}
           |"col16":{"cf":"pred", "col":"brk", "type":"int"}
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
                                
        def withCatalog(cat: String): DataFrame = {
          sqlContext
            .read
            .options(Map(HBaseTableCatalog.tableCatalog -> cat))
            .format("org.apache.spark.sql.execution.datasources.hbase")
            .load()
        }

        // mapper object created on each executor node
        val mapper = new ObjectMapper() // with ScalaObjectMapper
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        mapper.registerModule(DefaultScalaModule)

        // println("DBG BEF REPLACE tbl=" + hbaseTbl + "#tbl=" + tbl + "#cat=" + cat)
        mvg_avg_cat = mvg_avg_cat.replaceFirst("TBL", mvgAvgTbl)
        pred_cat = pred_cat.replaceFirst("TBL", predTbl)

        rdd.foreach(record => {

            var body: JsonNode = mapper.readTree(record).path("body")

            var bodyNode: JsonNode = body.get(Constants.BODY)
            var detectorNode: JsonNode = bodyNode.get(Constants.DECTECTOR)
            var decTypeCode: String = detectorNode.get(Constants.DEC_TYPE_CODE).asText()

            if (decTypeCode.toLowerCase() == Constants.DEC_TYPE_CODE_WILD.toLowerCase()) {

              var trainNode: JsonNode = bodyNode.get("train")

              var equipNodeArr: JsonNode = trainNode.get("equipments")

              for (equipNode: JsonNode <- equipNodeArr) {

                var wheelArr: JsonNode = equipNode.get("wheels")
                for (wheelNode: JsonNode <- wheelArr) {

                  var mrsArr: JsonNode = wheelNode.get("measurements")
                  for (mrs: JsonNode <- mrsArr) {
                    println("DBG: in Wheel meas..")

                    if (mrs.get("typeCode").asText().toLowerCase() == Constants.WHL_KIPS_DYN.toLowerCase()) {

                      var eqpInit: String = equipNode.get("initial").asText()
                      var eqpNumb: String = equipNode.get("number").asText()
                      var locCode: String = wheelNode.get("locationCode").asText()
                      var loadMty: String = equipNode.get("loadEmptyCode").asText()
                      var timeStamp: String = mrs.get("timestamp").asText()
                      var wheelKipsDyn: Double = mrs.get("strValue").asText().trim().toDouble

                      println("DBG: in Wheel meas.." + eqpInit + "#" + eqpInit + "#" + locCode + "#" + loadMty + "#" + timeStamp + "#" + wheelKipsDyn)

                      //								 wildDetectorBean.setInitial(initial)
                      //								 wildDetectorBean.setNumber(number)
                      //								 wildDetectorBean.setLocationCode(locationCode)
                      //								 wildDetectorBean.setCarId(initial+number)
                      //								 wildDetectorBean.setWheelId(initial+number+locationCode)
                      //								 wildDetectorBean.setLoadMty(loadMty)
                      //								 wildDetectorBean.setTimestamp(timestamp)
                      //								 wildDetectorBean.setWheelKipsDyn(Double.parseDouble(wheelKipsDyn))
                      //								 wildDetectorBeans.add(wildDetectorBean)

                      //								 detectDefectWhls(wildDetectorBean)

                      // val rowkey = eqpInit + "-" + eqpNumb + "-" + locCode + "-" + loadMty
                      val rowkey = "WFAX-      6016-R4-U"

                      val df = withCatalog(mvg_avg_cat)
                      df.registerTempTable("mvg_avg")

                      var result = sqlContext.sql("select * from mvg_avg where col0 = '" + rowkey + "'")	//.repartition(1)

                      result.cache()
                      
                      case class BrokenWhl(key: String,
                                ts1: String, rd1: Double, avg1: Double, //ts1 is Most recent readings to ts5 is least reading
                                ts2: String, rd2: Double, avg2: Double,
                                ts3: String, rd3: Double, avg3: Double,
                                ts4: String, rd4: Double, avg4: Double,
                                ts5: String, rd5: Double, avg5: Double,
                                brk: Double)
                                
                      val predRowRDD = result.map{
                        
                        row =>
                          {

                            val brk: Int = detectDefectWhls(row, wheelKipsDyn, timeStamp)

                            row match {

                              case Row(key: String,
                                ts1: String, rd1: Double, avg1: Double, //ts1 is Most recent readings to ts5 is least reading
                                ts2: String, rd2: Double, avg2: Double,
                                ts3: String, rd3: Double, avg3: Double,
                                ts4: String, rd4: Double, avg4: Double,
                                ts5: String, rd5: Double, avg5: Double) => {
                                  
                                  var newAvg = (wheelKipsDyn + rd1 + rd2)/3.0
                                  
                                  println("DBG in calc loop#" + key + "#" + timeStamp + "#" + wheelKipsDyn + "#" + newAvg + "#" + ts1 + "#" + rd1 + "#" + avg1 + "#" + ts2 + "#" + rd2 + "#" + avg2 + "#" + ts3 + "#" + rd3 + "#" + avg3 + "#" + ts4 + "#" + rd4 + "#" + avg4)
                                  Row(key, timeStamp, wheelKipsDyn, newAvg, ts2, rd2, avg2, ts3, rd3, avg3, ts4, rd4, avg4, ts5, rd5, avg5, brk)
                                  
                              }
                            }
                          }
                      }

                      println("DBG: after getRecord.." + result.rdd.getNumPartitions)
                      sqlContext.createDataFrame(predRowRDD, predSchema).write
                                .options(Map(HBaseTableCatalog.tableCatalog -> pred_cat, HBaseTableCatalog.newTable -> "1"))
                                .format("org.apache.spark.sql.execution.datasources.hbase")
                                .save()

                      val updRowRDD = result.map {
                        
                              case Row(key: String,
                                ts1: String, rd1: Double, avg1: Double, //ts1 is Most recent readings to ts5 is least reading
                                ts2: String, rd2: Double, avg2: Double,
                                ts3: String, rd3: Double, avg3: Double,
                                ts4: String, rd4: Double, avg4: Double,
                                ts5: String, rd5: Double, avg5: Double) => {
                                  
                                  var newAvg = (wheelKipsDyn + rd1 + rd2)/3.0
                                  println("DBG in upd loop#" + key + "#" + timeStamp + "#" + wheelKipsDyn + "#" + newAvg + "#" + ts1 + "#" + rd1 + "#" + avg1 + "#" + ts2 + "#" + rd2 + "#" + avg2 + "#" + ts3 + "#" + rd3 + "#" + avg3 + "#" + ts4 + "#" + rd4 + "#" + avg4)
                                  Row(key, timeStamp, wheelKipsDyn, newAvg, ts1, rd1, avg1, ts2, rd2, avg2, ts3, rd3, avg3, ts4, rd4, avg4)
                                  
                              }
                      }
                      
                      sqlContext.createDataFrame(updRowRDD, updSchema).write
                                .options(Map(HBaseTableCatalog.tableCatalog -> mvg_avg_cat, HBaseTableCatalog.newTable -> "1"))
                                .format("org.apache.spark.sql.execution.datasources.hbase")
                                .save()

                    }//If wheel kips dyn

                  }//Wheel Meas

                }//Wheel

              }//Eqp

            }//WILD

        }) //map record in Parn


      })//foreachRDD

    println("DBG: post processAlgo..")

  }

  def getTimeDifference(curr_ts: String, rcnt_ts: String): Long = {

    val fmt: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    var diff: Long = -99999

    try {
      var currDt : Date = fmt.parse(curr_ts)
      var rcntDt : Date = fmt.parse(rcnt_ts)
      var dayMilliSecs : Long = 24*60*60*1000
      
      diff = (currDt.getTime - rcntDt.getTime)/dayMilliSecs
      
    } catch {
      case ex: Exception => {
        println("DBG in createContext Caught Final Exception")
        ex.printStackTrace()
        diff = -99999
      }
    }

    diff
  }

  def detectDefectWhls(row: Row, wheelKipsDyn: Double, curr_ts: String): Int = {

    row match {
      case Row(key: String,
        ts1: String, rd1: Double, avg1: Double, //ts1 is Most recent readings to ts5 is least reading
        ts2: String, rd2: Double, avg2: Double,
        ts3: String, rd3: Double, avg3: Double,
        ts4: String, rd4: Double, avg4: Double,
        ts5: String, rd5: Double, avg5: Double) => {

        // -99999.0 is default for NULL values or less than 3 readings)
        var newAvg: Double = -99999.0
        var brk: Int = -99999

        //Initial null readings will be defaulted to 99999 while inserting by Upstream Application 
        if (rd1 != -99999.0 && rd2 != -99999.0) {

          newAvg = (wheelKipsDyn + rd1 + rd2) / 3

          var timeDiff: Long = getTimeDifference(curr_ts, ts2)
          //Check if wheel is broken
          //Broken Wheel is probable if 
          //mvgAvg with current reading [Avg(rd(i) , rd(i-1), rd(i-2)), is 'i' current rdg] in JSON 
          // is greater than it's 2nd most recent reading [Avg(i-2)]
          //The date time [i] - [i-1] < 60 Days and 
          // then Avg(i) >= Avg(i-1) >= Avg(i-2)
          // and then Avg(i) - Avg(i-2) > 5
          if ((newAvg - avg2 > 5.0) &&
            (newAvg >= avg1) && (avg1 >= avg2) && (timeDiff < 60) ){
            brk = 1
          }

        }
        
        brk
      }
    }
  }

  def createContext(duration: Long, checkpointDirectory: String, hdfsPath: String, hbaseTbl: String): StreamingContext = {

    // If you do not see this printed, that means the StreamingContext has been loaded
    // from the new checkpoint
    println("Creating new context")

    val sparkConf = new SparkConf()

    // Create the context with second batch size
    val ssc = new StreamingContext(sparkConf, Seconds(duration))

    var hBaseWrite : Boolean = false
    var hdfsWrite : Boolean = false

    try {

      //Set up Kerberos UGI

      if (UserGroupInformation.isSecurityEnabled) {
        if (principal == null || keyTab == null) {
          throw new Error(" You have to specify keytab and principal when security is enabled ")
        }
      }
      val ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keyTab)
      UserGroupInformation.setLoginUser(ugi)

      ssc.checkpoint(checkpointDirectory)

      val msgDStream = ssc.receiverStream(new WildMessageReceiverHDP())
      // val unionStream = ssc.union(msgDStream)

      println(System.currentTimeMillis() + "DBG msgStream count = " + msgDStream.count())

      ugi.doAs(new PrivilegedAction[Unit] {
        override def run: Unit = {

          ugi.checkTGTAndReloginFromKeytab()

          try {

            hBaseWrite = false
            hdfsWrite = false

            hBaseWrite = WriteToHBase(msgDStream, hbaseTbl)

            msgsPath = hdfsPath
            if ( hBaseWrite == true )
               hdfsWrite = WriteToHDFS(msgDStream, msgsPath)

          } catch {

            case ex: Exception => {

              println("DBG::createContext::Exception caught in createContext..check which action failed..")
              ex.printStackTrace()

              if ( hBaseWrite == false ){

                println("DBG::createContext::Exception writing to HBASE .. will attempt to write messages to HDFS to error path :" + errPath)
                println("DBG::createContext::Exception writing to HBASE .. please review the exception reported above")

                // Change path to errPath and attempt to write to HDFS
                msgsPath = errPath

                //On Hbase Exception Write records to HDFS
                //If HDFS throws exception then try aborting a Hbase Exception Write records to HDFS
                //If HDFS write fails it throws exception which is caught in outer try/catch block
                hdfsWrite = WriteToHDFS(msgDStream, msgsPath)

              }
              //This is false only if HDFS throw exception from try Block
              if ( hdfsWrite == false ){
                 //Do not throw exception if HBase was OK
                 if ( hBaseWrite == true ){
                   println("DBG::createContext::Exception writing to HDFS .. as HBase is OK , will continue to process..")
                   println("DBG::createContext::Exception writing to HDFS .. please review the exception reported above")
                 }
              }

            }
          }

        //Process algorithm
        // processWILDAlgo(rdd)

        }
      })

    } catch {
      case ex: Exception => {
        println("DBG::createContext::Exception caught in outer block..stop if both HBase/HDFS action failed..")
        ex.printStackTrace()

        //Throw exception only if both HBase/HDFS writes fail
        if ( hdfsWrite == false && hBaseWrite == false ){
             throw ex
        }
      }
    }

    ssc

  }

  //Read up properties 
  val propFileName = "/config/config.properties"

  val inputStream: InputStream = this.getClass.getResourceAsStream(propFileName)

  var appProp: Properties = new Properties()
  appProp.load(inputStream)

  //Spark Streaming variables
  val duration: Long = appProp.getProperty("receiver.batch.duration.seconds").toLong
  val checkpointPath = appProp.getProperty("receiver.checkpoint.dir")

  //By default set msgsPath to msgs dir , change it on exception writing to HBase
  var msgsPath = appProp.getProperty("receiver.hdfs.msgs.dir")
  val hdfsPath = appProp.getProperty("receiver.hdfs.msgs.dir")
  val errPath = appProp.getProperty("receiver.hdfs.err.msgs.dir")
  val driverStopFile = appProp.getProperty("driver.stop.file")
  val recvStopFile = appProp.getProperty("receiver.stop.file")

  //Kerberos principal/keytab file
  val principal = appProp.getProperty("receiver.hbase.principal")
  val keyTab = appProp.getProperty("receiver.hbase.keyTab")
  
  //Hbase table names
  val hbaseTbl = appProp.getProperty("receiver.hbase.detr_msgs.table")
  val mvgAvgTbl = appProp.getProperty("receiver.hbase.whl_avg.table")
  val predTbl = appProp.getProperty("receiver.hbase.whl_pred.table")

  def createNewSSC() : StreamingContext = {
      val ssc = createContext(duration, checkpointPath, hdfsPath, hbaseTbl)
      ssc
  }

  // check every 10s for shutdown hdfs file
  val checkIntervalMillis = 10000
  
  def fileExists(file: String): Boolean = {
    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    hadoopConf.addResource(new Path("/etc/hadoop/conf/core-site.xml"))
    hadoopConf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"))

    val fs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    val exists = fs.exists(new org.apache.hadoop.fs.Path(file))
    return exists
  }

  def isShutdownRequested(file:String ): Boolean = {
    val exists = fileExists(file)
    return exists
  }

  def main(args: Array[String]) {

    try {

      // SOP("In readProperties without params cfg file=" + propFileName)

      println("DBG receiver.hbase.table=" + hbaseTbl + "princ=" + principal + "#keyTab=" + keyTab + "#hdfsPath=" + hdfsPath + "#checkpointPath=" + checkpointPath + "#duration=" + duration)

      // val ssc =  StreamingContext.getOrCreate(checkpointPath, createNewSSC)

      //Parallel & multiple Receivers
      // val numStreams = appProp.getProperty("receiver.multiple.streams")
      // Define as many checkpoint directories as numStreams and pass to each Receiver below
      // val ssc = (1 to numStreams).map( itr => 
      //		StreamingContext.getOrCreate(appProp.getProperty("receiver.checkpoint.dir." + itr), 
      //		() => createContext(duration, appProp.getProperty("receiver.checkpoint.dir." + itr), hdfsPath, hbaseTbl)) )

      //Currently one Receiver should suffice
      // val ssc = StreamingContext.getOrCreate(checkpointPath, () => createContext(duration, checkpointPath, hdfsPath, hbaseTbl))
      val ssc = StreamingContext.getOrCreate(checkpointPath, createNewSSC)

      //Start the context
      ssc.start()

      //Wait till application terminates on call of stop()
      if ( ssc != null) {
        ssc.awaitTermination()
        println("DBG In main after awaitTermination() ")
      }

      //TODO test before Shutdown hook in case of forced failure of Driver
      sys.ShutdownHookThread {
        println("DBG In sys.ShutdownHookThread Gracefully stopping Spark Streaming Application")
        val stopSparkContext = true
        val stopGracefully = true
        println("DBG In sys.ShutdownHookThread Stopping SSC gracefully now..")
        ssc.stop(stopSparkContext, stopGracefully)
      }

      println("DBG In main after start() before awaitTermn")

      if (isShutdownRequested(driverStopFile)) {
        val stopSparkContext = true
        val stopGracefully = true
        println("DBG isShutdownRequested Found Driver Stopping SSC gracefully now..")
        println("DBG Found DRIVER STOP file, initiating Driver shutdown to stop receiving messages from TIBCO..")
        ssc.stop(stopSparkContext, stopGracefully)
      }


      println("DBG Stopping Driver ..")

    } catch {
      case ex: Exception => {
        println("DBG in main Caught Final Exception")
        ex.printStackTrace()
        throw ex
      }
    }

  }

  case class JMS_Message(key: String, msg: String)

  /*

rowkey= <detrTyp>-<msgid>

column=hdr:<detr_typ>
column=hdr:<msg_ver>  - to be added
column=hdr:<msg_id>  - to be added
column=hdr:<trn_id>  - to be added
column=hdr:<trn_ps_ts>  - to be added
column=hdr:<dtr_id>  - to be added
column=hdr:<jms_ts>  - to be added

column=dtl:<msg>

column=status:<stat_cd>
column=status:<cret_ts> - to be added

*/

  // case class Detector_Message ( rowkey: String, detr_typ: String, msg_ver: String, msg_id: Long, trn_id: String, trn_ps_ts: String, detr_id: String, msg : String, stat_cd: String, cret_ts: Long)
  case class Detector_Message(col0: String, col1: String, col2: String, col3: Long, col4: String, col5: String, col6: String, col7: String, col8: String, col9: Long)

  object Detector_Message {
    def apply(col0: String, col1: String, col2: String, col3: Long, col4: String, col5: String, col6: Long, 
              col7: String, col8: String, col9: Long): Detector_Message ={
      Detector_Message(col0, col1, col2, col3, col4, col5, col6, col7, col8, col9)
    }
  }

}

// class WildMessageReceiverHDP(val propFileName: String) extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) {
class WildMessageReceiverHDP extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) {
  // StorageLevel.MEMORY_AND_DISK_SER

  var foreignProviderFactory: String = null

  var foreignProviderURL: String = null

  var connectionFactoryName: String = null

  var destinationName: String = null

  var userName: String = null

  var password: String = null

  var iterations: Int = 1

  var ctxForeign: Context = null

  var usingLDAP: Boolean = false

  var lookupName: String = _

  var connectionRecv: javax.jms.Connection = null
  var sessionRecv: javax.jms.Session = null
  var msgConsumer: javax.jms.MessageConsumer = null

  var sparkProviderURL: String = null
  var host: String = null
  var port: Int = -1

  var factory: javax.jms.ConnectionFactory = null
  var destination: Destination = null
  var appProp: Properties = null

  val ctxtFactory = "provider.context.factory"
  val providerURL = "provider.url"
  val jndiCtxtLookup = "jndi.context.lookup"
  val destName = "destination.name"
  val username = "destination.username"
  val passwd = "destination.password"

  val skipReceive = "skip.receive"
  var recvStopFile = ""

  val initialized = init()

  def init() {
    // SOP("In init .. readProperties without params")
    readProperties()
  }

  def readProperties() {

    try {

      val propFileName = "/config/config.properties"
      // SOP("In readProperties without params cfg file=" + propFileName)

      val inputStream: InputStream = this.getClass.getResourceAsStream(propFileName)

      appProp = new Properties()

      appProp.load(inputStream)

      foreignProviderFactory = appProp.getProperty(ctxtFactory)
      foreignProviderURL = appProp.getProperty(providerURL)
      connectionFactoryName = appProp.getProperty(jndiCtxtLookup)


      destinationName = appProp.getProperty(destName)
      userName = appProp.getProperty(username)
      password = appProp.getProperty(passwd)

      recvStopFile = appProp.getProperty("receiver.stop.file")

      // SOP("In read properties")
      // SOP("Factory=" + foreignProviderFactory + "#URL" + foreignProviderURL + "#JNDILookUp=" + connectionFactoryName + "#TOPIC_QUEUE=" + destinationName + "#Usr=" + userName + "#pass=" + password)
    } catch {
      case io: IOException => {
        println("IOException reading Properties file..restarting" + io.getMessage)
        io.printStackTrace()
        throw io
      }
      case ex: Exception => {
        println("Exception reading Properties file data..restarting" + ex.getMessage)
        ex.printStackTrace()
        throw ex
      }

    }

  }

  def createInitialContext(contextFactory: String,
    providerUrl: String,
    userName: String,
    password: String): Context = {
    val env = new Hashtable[String, String]()
    env.put(Context.INITIAL_CONTEXT_FACTORY, contextFactory)
    env.put(Context.PROVIDER_URL, providerUrl)
    env.put(Context.SECURITY_PRINCIPAL, userName)
    env.put(Context.SECURITY_CREDENTIALS, password)
    env.put(Context.REFERRAL, "throw")
    val ctx = new InitialContext(env)
    ctx
  }

  // init()

  def receive() {

    // readProperties(propFileName)
    readProperties()

    if (appProp.getProperty(skipReceive) != null) {
      stop("Probable version change, to recover earlier messages, this run will skip messages from TIBCO..")
      return
    }

    // SOP("Using JNDI to read objects from a foreign naming/directory service sample.")
    // SOP("Using server: " + foreignProviderURL)

    var factory: javax.jms.ConnectionFactory = null
    var destination: Destination = null

    if (foreignProviderURL.substring(0, 5) == "ldap:") usingLDAP = true

    try {

      // SOP("Factory=" + foreignProviderFactory + "#URL" + foreignProviderURL + "#JNDILookUp=" + connectionFactoryName + "#TOPIC_QUEUE=" + destinationName + "#Usr=" + userName + "#pass=" + password)

      ctxForeign = createInitialContext(foreignProviderFactory, foreignProviderURL, userName, password)
      ctxForeign.addToEnvironment(Context.OBJECT_FACTORIES, "com.tibco.tibjms.naming.TibjmsObjectFactory")
      ctxForeign.addToEnvironment(Context.URL_PKG_PREFIXES, "com.tibco.tibjms.naming")

      //        for (i <- 0 until iterations) {
      if (connectionFactoryName != null) {
        lookupName = if ((usingLDAP)) "cn=" + connectionFactoryName else connectionFactoryName
        factory = ctxForeign.lookup(lookupName).asInstanceOf[javax.jms.ConnectionFactory]
        // SOP("looked up connection factory = " + factory)
      }
      if (destinationName != null) {
        lookupName = if ((usingLDAP)) "cn=" + destinationName else destinationName
        destination = ctxForeign.lookup(lookupName).asInstanceOf[Destination]
        // SOP("looked up destination = " + destination)
      }
      //        }

      if (factory.isInstanceOf[javax.jms.ConnectionFactory] && destination != null) {
        val ackMode = com.tibco.tibjms.Tibjms.EXPLICIT_CLIENT_ACKNOWLEDGE	//Session.CLIENT_ACKNOWLEDGE
        val cf: javax.jms.ConnectionFactory = factory.asInstanceOf[javax.jms.ConnectionFactory]
        connectionRecv = cf.createConnection(userName, password)
        sessionRecv = connectionRecv.createSession(ackMode)
        msgConsumer = sessionRecv.createConsumer(destination)

        var msgList = ArrayBuffer[String]()
        var cnt = 0

        // SOP("Past consumer..")

        connectionRecv.start()

        breakable {

          //Check if we should stop receiving any more messages or continue
          while (!isStopped()) {

            // SOP("Waiting to receive messages.. ")
            val msg = msgConsumer.receive()
            if (msg == null) {
              stop("Stopping receiving messages from TIBCO..")
              break
            }

            val txt = msg.asInstanceOf[TextMessage]
            System.out.println("JMS Props@corrID=" + msg.getJMSCorrelationID() + "#msgID=" + msg.getJMSMessageID() + "#msgType=" + msg.getJMSType() + "#msgTimestamp=" + msg.getJMSTimestamp()) //                + "#="

            // store(txt.getText)
            // store( JMS_Message (msg.getJMSCorrelationID(), txt.getText))
            store(txt.getText)
         
            //Acknowledge message to queue 
            msg.acknowledge()

          }

          // If control is here, then stop file was found
          println("DBG Found RECEIVER STOP file, initiating Receiver shutdown to stop receiving messages from TIBCO..")
          stop("Stopping receiving messages from TIBCO..")

        }

      }
    } catch {
      case e: JMSException => {
        println("JMSException receiving data..restarting" + e.getMessage)
        e.printStackTrace()
        //restart("JMSException receiving data..restarting", e)
        throw e
      }
      case e: NamingException => {
        println("NamingException receiving data..restarting" + e.getMessage)
        e.printStackTrace()
        //restart("NamingException receiving data..restarting", e)
        throw e
      }
      case t: Throwable => {
        println("General Error receiving data " + t.getMessage)
        t.printStackTrace()
        //restart( "General Error receiving data " + t.getMessage, t)
        throw t
      }

    } finally {

      if (connectionRecv != null)
        connectionRecv.close()

      if (sessionRecv != null)
        sessionRecv.close()

      if (msgConsumer != null)
        msgConsumer.close()

    }
  }

  var receiver : Thread = null

  def onStart() {

    if (!isStopped()) {

      receiver = new Thread() {

        override def run() {
          // while(! isStopped() ) {
             println("Calling receive in thread..")
             receive()
          //}
        }
      }
      receiver.start()
    }
  }

  def onStop() {

    println("DBG stopping the Receiver")
    try {
      if (connectionRecv != null)
        connectionRecv.close()

      if (sessionRecv != null)
        sessionRecv.close()

      if (msgConsumer != null)
        msgConsumer.close()

      //Sleep for 60s to allow flush of messages
      receiver = null
      Thread.sleep(60)
      println("DBG Stopped the Receiver thread!! ")

    } catch {
      case e: JMSException => {
        println("JMSException in stop() " + e.getMessage)
        e.printStackTrace()
        throw e
      }
    }
    println("DBG closed all connections!!")
  }

   override def isStopped(): Boolean = {

      val exists = fileExists(recvStopFile)
      exists
  }

  def fileExists(file: String): Boolean = {

    val hadoopConf = new org.apache.hadoop.conf.Configuration()

    hadoopConf.addResource(new Path("/etc/hadoop/conf/core-site.xml"))
    hadoopConf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"))

    val fs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    val exists = fs.exists(new org.apache.hadoop.fs.Path(file))

    return exists

  }

}

