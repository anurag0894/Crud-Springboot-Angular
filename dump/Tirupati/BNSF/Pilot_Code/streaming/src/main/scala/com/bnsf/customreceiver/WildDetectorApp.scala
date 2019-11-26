package com.bnsf.customreceiver

//import com.bnsf.model.ioc.Message
//import com.bnsf.model.ioc.Header

import java.util.Hashtable
//remove if not needed
import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.ListBuffer

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
import org.apache.spark.sql.types.{ StructType, StructField, StringType, DoubleType }
import org.apache.spark.sql.hive.HiveContext

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
import java.io.FileInputStream

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

object WildDetectorApp {

/*
  val DELIMITER = '\u0001'
  
  //Read up properties 
  // val propFileName = "/config/config.properties"
  // val inputStream: InputStream = this.getClass.getResourceAsStream(propFileName)

  val propFileName = "wild.properties"
  val inputStream: FileInputStream = new FileInputStream(propFileName)
  var appProp = new Properties()
  appProp.load(inputStream)
  inputStream.close()

  //Spark Streaming variables
  val duration: Long = appProp.getProperty("receiver.batch.duration.seconds").toLong
  val checkpointPath = appProp.getProperty("receiver.checkpoint.dir")

  //By default set msgsPath to msgs dir , change it on exception writing to HBase
  var msgsPath = appProp.getProperty("receiver.hdfs.msgs.dir")
  val hdfsPath = appProp.getProperty("receiver.hdfs.msgs.dir")
  val errPath = appProp.getProperty("receiver.hdfs.err.msgs.dir")
  val fileMsgPath = appProp.getProperty("receiver.hdfs.file.msgs.dir")
  val driverStopFile = appProp.getProperty("driver.stop.file")
  val recvStopFile = appProp.getProperty("receiver.stop.file")

  //Kerberos principal/keytab file
  val principal = appProp.getProperty("receiver.hbase.principal")
  val keyTab = appProp.getProperty("receiver.hbase.keyTab")

  //Hbase table names
  val hbaseTbl = appProp.getProperty("receiver.hbase.detr_msgs.table")
  val mvgAvgTbl = appProp.getProperty("receiver.hbase.whl_avg.table")
  val predTbl = appProp.getProperty("receiver.hbase.whl_pred.table")

  // check every 10s for shutdown hdfs file
  val checkIntervalMillis = 10000
*/
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
      col7: String, col8: String, col9: Long): Detector_Message = {
      Detector_Message(col0, col1, col2, col3, col4, col5, col6, col7, col8, col9)
    }
  }

case class MvgAvgRecord(col0: String, col1 : String, col2 : Double, col3 : Double, col4 : String, col5 : Double, col6 : Double, col7 : String, col8 : Double, col9 : Double, col10 : String, col11 : Double, col12 : Double, col13 : String, col14 : Double, col15 : Double)

object MvgAvgRecord {

	def apply(initial: String, number: String, locationCode: String, loadEmptyCode: String, col1 : String, col2 : Double, col3 : Double, col4 : String, col5 : Double, col6 : Double, col7 : String, col8 : Double, col9 : Double, col10 : String, col11 : Double, col12 : Double, col13 : String, col14 : Double, col15 : Double) : MvgAvgRecord = {
	  
	  var key =  StringBuilder.newBuilder
	  
	  key.append(initial).append("-").append(number).append("-").append(locationCode).append("-").append(loadEmptyCode)
	  
	  MvgAvgRecord(key.toString(), col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, col11, col12, col13, col14, col15)
	}

}

case class PredRecord(col0: String, col1 : String, col2 : Double, col3 : Double, col4 : String, col5 : Double, col6 : Double, col7 : String, col8 : Double, col9 : Double, col10 : String, col11 : Double, col12 : Double, col13 : String, col14 : Double, col15 : Double, col16 : Int, col17: String)

object PredRecord {

	def apply(initial: String, number: String, locationCode: String, loadEmptyCode: String, col1 : String, col2 : Double, col3 : Double, col4 : String, col5 : Double, col6 : Double, col7 : String, col8 : Double, col9 : Double, 
    col10 : String, col11 : Double, col12 : Double, col13 : String, col14 : Double, col15 : Double, col16 : Int, col17: String) : PredRecord = {
	  
	  var key =  StringBuilder.newBuilder
	  
	  key.append(initial).append("-").append(number).append("-").append(locationCode).append("-").append(loadEmptyCode)
	  
	  PredRecord(key.toString(), col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, col11, col12, col13, col14, col15, col16, col17)
	}

}

  val FAILURE : Int = -1
  val SUCCESS : Int = 0

  def getDateInHH(): String = {

    val now = Calendar.getInstance().getTime()
    val minuteFormat = new SimpleDateFormat("yyyyMMdd-HH")
    val currDateHr = minuteFormat.format(now)
    currDateHr
  }

  def buildMvgAvgList(iterator: Iterator[Row]): Iterator[MvgAvgRecord] = {

    val records = iterator.toList
    val result = records.map(record => {

      try {

        MvgAvgRecord(record.getString(0), record.getString(1), record.getString(2), record.getString(3), 
            record.getString(4), record.getDouble(5), record.getDouble(6), 
            record.getString(7), record.getDouble(8), record.getDouble(9),
                     record.getString(10), record.getDouble(11), record.getDouble(12), 
                     record.getString(13), record.getDouble(14), record.getDouble(15), 
                     record.getString(16), record.getDouble(17), record.getDouble(18))

        //   System.out.println(System.currentTimeMillis() + " DBG in buildMsgList message object is NULL ")

      } catch {
        case ex: Exception => {
          System.out.println(System.currentTimeMillis() + "::DBG::WildErrMsgProcessor::buildPredList::Exception " + ex.getMessage())
          ex.printStackTrace()
          throw ex
        }
      }
    }).iterator //End map

    result

  }

  def buildPredList(iterator: Iterator[Row]): Iterator[PredRecord] = {

    val records = iterator.toList
    val result = records.map(record => {

      try {

        PredRecord(record.getString(0), record.getString(1), record.getString(2), record.getString(3), 
            record.getString(4), record.getDouble(5), record.getDouble(6), 
            record.getString(7), record.getDouble(8), record.getDouble(9),
                     record.getString(10), record.getDouble(11), record.getDouble(12), 
                     record.getString(13), record.getDouble(14), record.getDouble(15), 
                     record.getString(16), record.getDouble(17), record.getDouble(18), record.getInt(19), record.getString(20) )

        //   System.out.println(System.currentTimeMillis() + " DBG in buildMsgList message object is NULL ")

      } catch {
        case ex: Exception => {
          System.out.println(System.currentTimeMillis() + "::DBG::WildErrMsgProcessor::buildPredList::Exception " + ex.getMessage())
          ex.printStackTrace()
          throw ex
        }
      }
    }).iterator //End map

    result

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
        // if (message != null)

        //From Header get MsgId and Detector type
        val msgId: Long = message.path("messageId").asLong()

        val msgType: String = message.path("type").asText()

        // System.out.println(System.currentTimeMillis() + " DBG in buildMsgList " + msgId + "#det=" + msgType)

        //Detector_Message(msgId + "-" + msgType , msgId, msgType, record)

        // case class Detector_Message ( rowkey: String, detr_typ: String, msg_ver: String, msg_id: Long, trn_id: String, trn_ps_ts: String, detr_id: String, msg : String, stat_cd: String, cret_ts: Long)
        Detector_Message(msgId + "-" + msgType, msgType, "msg_ver", msgId, "trn_id", "trn_ps_ts", "detr_id", record, "U", System.currentTimeMillis())

        //   System.out.println(System.currentTimeMillis() + " DBG in buildMsgList message object is NULL ")

      } catch {
        case ex: Exception => {
          System.out.println(System.currentTimeMillis() + "::DBG::WildDetectorApp::buildMsgList::Exception " + ex.getMessage())
          ex.printStackTrace()
          throw ex
        }
      }
    }).iterator //End map

    result

  }

  def buildMsgTuple(iterator: Iterator[String]): Iterator[String] = {

    val DELIMITER = '\u0001'

    // mapper object created on each executor node
    val mapper = new ObjectMapper() // with ScalaObjectMapper
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    mapper.registerModule(DefaultScalaModule)

    val records = iterator.toList
    val result = records.map(record => {

      try {

        val message: JsonNode = mapper.readTree(record).path("header")
        // if (message != null)

        //From Header get MsgId and Detector type
        val msgId: Long = message.path("messageId").asLong()

        val msgType: String = message.path("type").asText()

        val line = StringBuilder.newBuilder

        // System.out.println(System.currentTimeMillis() + " DBG in buildMsgTuple " + msgId + "#det=" + msgType)

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

      } catch {
        case ex: Exception => {
          System.out.println(System.currentTimeMillis() + "::DBG::WildDetectorApp::buildMsgTuple:: Exception " + ex.getMessage())
          ex.printStackTrace()
          throw ex
        }
      }
    }).iterator //End map

    result

  }

  def WriteToHBase(msgRDD : DStream[String], hbaseTbl : String, hdfsPath : String, errPath : String, fileMsgPath : String, mvgAvgTbl : String, predTbl: String, principal: String, keyTab: String): Int = {

    var hbaseWrite: Int = FAILURE

    msgRDD.foreachRDD { rdd =>

      var conf: org.apache.hadoop.conf.Configuration = HBaseConfiguration.create()

      // conf.addResource("hbase-site.xml")
      // conf.addResource("core-site.xml")
      // conf.set("hbase.client.keyvalue.maxsize", "0")
      // conf.addResource("/etc/hbase/conf/hbase-site.xml")
      // conf.addResource("/etc/hadoop/conf/core-site.xml")
 /* 
  System.out.println("BEFORE RpcCF=" + conf.get("hbase.rpc.controllerfactory.class") + "#hdpSecAuth=" + conf.get("hadoop.security.authentication") + "#hbSecAuth=" + conf.get("hbase.security.authentication") + "#max=" + conf.get("hbase.client.keyvalue.maxsize") + "#cltRet=" + conf.get("hbase.client.retries.number") + "#hndlrCnt=" + conf.get("hbase.regionserver.handler.count") + "#rpcTO=" + conf.get("hbase.rpc.timeout") + "#jvKrb5=" + conf.get("java.security.krb5.conf") + "#hfVer=" + conf.get("hfile.format.version") + "#recvRetry=" + conf.get("zookeeper.recovery.retry") + "#zkTO=" + conf.get("zookeeper.session.timeout")  + "#zkQ=" + conf.get("hbase.zookeeper.quorum"))

      conf.addResource(new Path(System.getenv("PWD")+"/"+"core-site.xml"))
      conf.addResource(new Path(System.getenv("PWD")+"/"+"hbase-site.xml"))

  System.out.println("AFTER set files RpcCF=" + conf.get("hbase.rpc.controllerfactory.class") + "#hdpSecAuth=" + conf.get("hadoop.security.authentication") + "#hbSecAuth=" + conf.get("hbase.security.authentication") + "#max=" + conf.get("hbase.client.keyvalue.maxsize") + "#cltRet=" + conf.get("hbase.client.retries.number") + "#hndlrCnt=" + conf.get("hbase.regionserver.handler.count") + "#rpcTO=" + conf.get("hbase.rpc.timeout") + "#jvKrb5=" + conf.get("java.security.krb5.conf") + "#hfVer=" + conf.get("hfile.format.version") + "#recvRetry=" + conf.get("zookeeper.recovery.retry") + "#zkTO=" + conf.get("zookeeper.session.timeout") + "#zkQ=" + conf.get("hbase.zookeeper.quorum"))

      conf.set("hbase.zookeeper.property.clientPort", "2181" )
      conf.set("hbase.zookeeper.quorum", "toplxhdmd001.rails.rwy.bnsf.com,toplxhdmd002.rails.rwy.bnsf.com,toplxhdmd003.rails.rwy.bnsf.com")

      conf.set("hadoop.security.authentication", "kerberos")
      conf.set("hbase.security.authentication", "kerberos")
      conf.set("hbase.client.keyvalue.maxsize", "20971520")
      conf.set("hbase.client.retries.number", "0")
      conf.set("hbase.regionserver.handler.count", "60")
      conf.set("hbase.rpc.timeout", "30000")
      conf.set("hbase.rpc.controllerfactory.class", "org.apache.hadoop.hbase.ipc.RpcControllerFactory")
      conf.set("java.security.krb5.conf", "/etc/krb5.conf")
      conf.set("hfile.format.version", "3")
      conf.set("zookeeper.recovery.retry", "0")
      conf.set("zookeeper.session.timeout", "30000")

System.out.println("AFTER Appln RpcCF=" + conf.get("hbase.rpc.controllerfactory.class") + "#hdpSecAuth=" + conf.get("hadoop.security.authentication") + "#hbSecAuth=" + conf.get("hbase.security.authentication") + "#max=" + conf.get("hbase.client.keyvalue.maxsize") + "#cltRet=" + conf.get("hbase.client.retries.number") + "#hndlrCnt=" + conf.get("hbase.regionserver.handler.count") + "#rpcTO=" + conf.get("hbase.rpc.timeout") + "#jvKrb5=" + conf.get("java.security.krb5.conf") + "#hfVer=" + conf.get("hfile.format.version") + "#recvRetry=" + conf.get("zookeeper.recovery.retry") + "#zkTO=" + conf.get("zookeeper.session.timeout")  + "#zkQ=" + conf.get("hbase.zookeeper.quorum") )
*/
      UserGroupInformation.setConfiguration(conf)

      val ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keyTab)
      UserGroupInformation.setLoginUser(ugi)

      ugi.doAs(new PrivilegedAction[Unit] {
        
        override def run: Unit = {

          ugi.checkTGTAndReloginFromKeytab()
  
    System.out.println(System.currentTimeMillis() + "::DBG::WildDetectorApp::WriteToHBase::principal=" + principal + "#keytab="+ keyTab + "#hdfsPath=" + hdfsPath + "#errPath=" + errPath + "#fileMsgPath=" + fileMsgPath + "#hbaseTbl=" + hbaseTbl ) //+ "#predTbl=" + predTbl ) //+ "#runMode=" + runMode)

          // Get the singleton instance of SQLContext
          val sqlContext = SQLContext.getOrCreate(rdd.sparkContext)
          import sqlContext.implicits._
  
          try {
  
            hbaseWrite = FAILURE
            if(! rdd.isEmpty() ) {
      
      // System.out.println(System.currentTimeMillis() + "::DBG::WildDetectorApp::WriteToHBase:: before PRINT ")
      // rdd.collect.foreach(print)
      // System.out.println(System.currentTimeMillis() + "::DBG::WildDetectorApp::WriteToHBase:: after PRINT ")

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
      
              cat = cat.replaceFirst("TBL", hbaseTbl)
      
              var result = rdd.mapPartitions(buildMsgList).toDF
      
              // for testing connection sharing only
              // result.toDF.repartition(1).write.options
              /* result.toDF.write.options(
                Map(HBaseTableCatalog.tableCatalog -> cat, HBaseTableCatalog.newTable -> "2"))
                .format("org.apache.spark.sql.execution.datasources.hbase")
                .save() */

              System.out.println(System.currentTimeMillis() + "::DBG::WildDetectorApp::WriteToHBase:: BEFORE Parallelize write to HBase .." + result.rdd.getNumPartitions + "#hdfsPath=" + hdfsPath + "#errPath=" + errPath + "#fileMsgPath=" + fileMsgPath + "#hbaseWrite=" + hbaseWrite )

              var dfWriter : DataFrameWriter = result.write
              dfWriter.options(
                Map(HBaseTableCatalog.tableCatalog -> cat))
                .format("org.apache.spark.sql.execution.datasources.hbase")
                .save()
      
              hbaseWrite = SUCCESS
              System.out.println(System.currentTimeMillis() + "::DBG::WildDetectorApp::WriteToHBase:: AFTER Parallelize write to HBase .." + result.rdd.getNumPartitions + "#hdfsPath=" + hdfsPath + "#errPath=" + errPath + "#fileMsgPath=" + fileMsgPath + "#hbaseWrite=" + hbaseWrite)
  
              // Write to HDFS Regular Path
              var records = rdd.mapPartitions(buildMsgTuple).toDF //.repartition(1)
              // records.toDF.repartition(1).write.mode(SaveMode.Append).format("text").save(hdfsPath + "/cret_ts=" + System.currentTimeMillis() + "/")
              records.write.mode(SaveMode.Append).format("text").save(hdfsPath + "/cret_ts=" + System.currentTimeMillis() + "/")
  
              System.out.println(System.currentTimeMillis() + "::DBG::WildDetectorApp::WriteToHBase:: Write to HDFS .." + records.rdd.getNumPartitions)
  
              // processAlgo(rdd, mvgAvgTbl, predTbl, principal, keyTab)

              // System.out.println(System.currentTimeMillis() + "::DBG::WildDetectorApp::WriteToHBase:: Completed processAlgo .." )
  
              //This should reset the connections on 
              dfWriter = null
              result = null

              records = null

            } //If not empty write
            hbaseWrite = SUCCESS
  
          } catch {
  
            case ex: Exception => {
  
              ex.printStackTrace()
  
              if ( hbaseWrite == FAILURE ) {
                System.out.println(System.currentTimeMillis() + "::DBG::WildDetectorApp::WriteToHBase:: In Exception attempting to write to HDFS : " + errPath + "# Exception is " + ex.getMessage() )
                try {
                  //Write to HDFS Error Path
                  //If Write to Error Path also fails then stop driver
                  var records = rdd.mapPartitions(buildMsgTuple).toDF //.repartition(1)
                  records.write.mode(SaveMode.Append).format("text").save(errPath + "/cret_ts=" + System.currentTimeMillis() + "/")
  
                  hbaseWrite = SUCCESS
                  records = null
                } catch {
  
                  case ex: Exception => {
  
                    hbaseWrite = FAILURE
                    System.out.println(System.currentTimeMillis() + "::DBG::WildDetectorApp::WriteToHBase:: Exception writing to HDFS, hbaseWrite=" + hbaseWrite + "# Exception is " + ex.getMessage() )
                    ex.printStackTrace()
  
                    throw ex
                  }
                } 
              } else
                 System.out.println(System.currentTimeMillis() + "::DBG::WildDetectorApp::WriteToHBase:: Exception writing to HDFS , hbaseWrite=" + hbaseWrite)
              // throw ex
  
            }
  
          }
          
        }

      } )
      conf = null
    } //foreachRDD

    hbaseWrite

  }

  def processAlgo(rdd: RDD[String], mvgAvgTbl: String, predTbl: String, principal : String, keyTab: String ): Unit = {

    val currDateHr = getDateInHH()
       try{

        System.out.println(System.currentTimeMillis() + "::DBG::WildDetectorApp::processAlgo with args mvgAvgTbl=" + mvgAvgTbl + "#predTbl=" + predTbl + "#principal=" + principal + "#keyTab=" + keyTab)

        // Get the singleton instance of SQLContext
        val sqlContext = SQLHiveContextSingleton.getInstance(rdd.sparkContext)
        import sqlContext.implicits._

        def withCatalog(cat: String): DataFrame = {
          sqlContext
            .read
            .options(Map(HBaseTableCatalog.tableCatalog -> cat))
            .format("org.apache.spark.sql.execution.datasources.hbase")
            .load()
        }

        val jsonMsgs = sqlContext.read.json(rdd)
        jsonMsgs.registerTempTable("jsonMsgs")

        // System.out.println(System.currentTimeMillis() + "::DBG::WildDetectorApp::processAlgo Able to print records START")
        // jsonMsgs.collect.foreach(println)
        // System.out.println(System.currentTimeMillis() + "::DBG::WildDetectorApp::processAlgo Able to print records END")
      
        // "       from jsonMsgs LATERAL VIEW explode(body.train.equipments) t1 as eqp where header.type = 'WILD' " +
        val jsonWhlMeasDF = sqlContext.sql("select initial, number, loadEmptyCode, locationCode, trn_id, " +
          " whlMeas.strValue as strValue, whlMeas.typeCode as typeCode, whlMeas.timestamp as timestamp from ( " +
          "   select initial, number,  loadEmptyCode, whl.locationCode as locationCode, trn_id, " +
          "          whl.measurements as measurements from (select eqp.initial as initial, " +
          "          eqp.number as number , eqp.loadEmptyCode as loadEmptyCode, body.train.trainId as trn_id, eqp.wheels as wheels " +
          "       from jsonMsgs LATERAL VIEW explode(body.train.equipments) t1 as eqp where header.type = 'WILD' " +
          " ) tab1 LATERAL VIEW explode(tab1.wheels) t2 as whl  " +
          " ) tab2 LATERAL VIEW explode(measurements)  t3 as whlMeas " + 
          " where whlMeas.typeCode = 'whl_kips_dyn'")

        // System.out.println(System.currentTimeMillis() + "::DBG::WildDetectorApp::processAlgo Able to print records START")
        // jsonWhlMeasDF.collect.foreach(println)
        // System.out.println(System.currentTimeMillis() + "::DBG::WildDetectorApp::processAlgo Able to print records END")
      
        jsonWhlMeasDF.registerTempTable("jsonWhlMeasTbl")
        jsonWhlMeasDF.printSchema

        var mvgAvgCat = s"""{
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
  
        var predCat = s"""{
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
             |"col17":{"cf":"trn", "col":"trn_id", "type":"string"}
             |}
             |}""".stripMargin
  
        mvgAvgCat = mvgAvgCat.replaceFirst("TBL", mvgAvgTbl)

        println("DBG MVG_AVG before withCat =" + mvgAvgCat)
        val mvgAvgDF = withCatalog(mvgAvgCat)
        mvgAvgDF.registerTempTable("mvgAvgTbl")
        mvgAvgDF.printSchema
        println("DBG MVG_AVG Count =" + mvgAvgDF.count)

        // val tstDF = sqlContext.sql("select * from mvgAvgTbl ")
        // tstDF.collect.foreach(println)
        // tstDF.printSchema

        //Join with mvg_avg table
        //Column aliases ts1=>col1, ts2=>col4, ts3=>col7, ts4=>col10, ts5=>col13
        //Column aliases rd1=>col2, rd2=>col5, rd3=>col8, r4=>col11, rd5=>col14
        //Column aliases avg1=>col3, avg2=>col6, avg3=>col9, avg=>col12, avg5=>col15
	//Same SQL will be used to build pred and also update back Mvg_avg
        val joinAvgNewRdg = sqlContext.sql(" select  rdg.initial, rdg.number, rdg.locationCode, rdg.loadEmptyCode, " + 
	  " rdg.timestamp as col1, cast(rdg.strValue as double) as col2, " + 
	  " case when mvg.col2 is not NULL and mvg.col2 <> -99999.0 and mvg.col5 is not NULL and mvg.col2 <> -99999.0 then " + 
	  "          (cast(rdg.strValue as double) + mvg.col2 + mvg.col5)/3 else -99999.0 end as col3, " + 
	  " coalesce(mvg.col1,'') as col4, coalesce(mvg.col2, -99999.0) as col5, coalesce(mvg.col3, -99999.0) as col6, " +
	  " coalesce(mvg.col4, '') as col7, coalesce(mvg.col5, -99999.0) as col8, coalesce(mvg.col6, -99999.0) as col9, " +
	  " coalesce(mvg.col7, '') as col10, coalesce(mvg.col8, -99999.0) as col11, coalesce(mvg.col9, -99999.0) as col12, " + 
	  " coalesce(mvg.col10, '') as col13, coalesce(mvg.col11, -99999.0) as col14, coalesce(mvg.col12, -99999.0) as col15, " + 
	  " case when mvg.col2 is not NULL and mvg.col2 <> -99999.0 and mvg.col5 is not NULL and mvg.col5 <> -99999.0 and " +
          " 	(( (cast(rdg.strValue as double) + mvg.col2 + mvg.col5)/3 ) >= mvg.col6 and mvg.col6 >= mvg.col9 ) and " +
          " 		(( (cast(rdg.strValue as double) + mvg.col2 + mvg.col5)/3 ) - mvg.col6 >= 5.0 ) and " +
          " 		(datediff(rdg.timestamp, mvg.col4) <= 60 ) " +
          " 		then 1 else 0 end as col16, rdg.trn_id " +
          " from jsonWhlMeasTbl rdg left outer join mvgAvgTbl mvg on (concat(rdg.initial,'-',rdg.number,'-',rdg.locationCode, '-', rdg.loadEmptyCode) = mvg.col0) ")

        predCat = predCat.replaceFirst("TBL", predTbl)
        println("DBG JOIN jsonMsgTbl records " + predCat)

        println("DBG JOIN jsonMsgTbl count " + joinAvgNewRdg.count() )
	// joinAvgNewRdg.collect.foreach(println)

        joinAvgNewRdg.mapPartitions(buildPredList)
          .toDF
          .write.options(
          Map(HBaseTableCatalog.tableCatalog -> predCat, HBaseTableCatalog.newTable -> "2"))
          .format("org.apache.spark.sql.execution.datasources.hbase")
          .save()

        System.out.println(System.currentTimeMillis() + "::DBG::WildDetectorApp::processAlgo Write to PRED Table COMPLETED " )

        joinAvgNewRdg.mapPartitions(buildMvgAvgList)
          .toDF
          .write.options(
          Map(HBaseTableCatalog.tableCatalog -> mvgAvgCat, HBaseTableCatalog.newTable -> "2"))
          .format("org.apache.spark.sql.execution.datasources.hbase")
          .save()

        System.out.println(System.currentTimeMillis() + "::DBG::WildDetectorApp::processAlgo Write to MVG AVG Table COMPLETED " )

    } catch {
      case ex: Exception => {
        System.out.println(System.currentTimeMillis() + "::DBG::WildDetectorApp::processAlgo Exception, reason: " + ex.getMessage)
        ex.printStackTrace()
        throw ex
      }
    }

  }
}

class WildDetectorApp (sc: SparkContext, sqlContext: SQLContext, 
      runMode: Int) {

  import WildDetectorApp._
  
    val DELIMITER = '\u0001'
  
  //Read up properties 
  // val propFileName = "/config/config.properties"
  // val inputStream: InputStream = this.getClass.getResourceAsStream(propFileName)

  val propFileName = "wild.properties"
  val inputStream: FileInputStream = new FileInputStream(propFileName)
  var appProp = new Properties()
  appProp.load(inputStream)
  inputStream.close()

  //Spark Streaming variables
  val duration: Long = appProp.getProperty("receiver.batch.duration.seconds").toLong
  val checkpointPath = appProp.getProperty("receiver.checkpoint.dir")

  //By default set msgsPath to msgs dir , change it on exception writing to HBase
  var msgsPath = appProp.getProperty("receiver.hdfs.msgs.dir")
  val hdfsPath = appProp.getProperty("receiver.hdfs.msgs.dir")
  val errPath = appProp.getProperty("receiver.hdfs.err.msgs.dir")
  val fileMsgPath = appProp.getProperty("receiver.hdfs.file.msgs.dir")
  val driverStopFile = appProp.getProperty("driver.stop.file")
  val recvStopFile = appProp.getProperty("receiver.stop.file")

  //Kerberos principal/keytab file
  val principal = appProp.getProperty("receiver.hbase.principal")
  val keyTab = appProp.getProperty("receiver.hbase.keyTab")

  //Hbase table names
  val hbaseTbl = appProp.getProperty("receiver.hbase.detr_msgs.table")
  val mvgAvgTbl = appProp.getProperty("receiver.hbase.whl_avg.table")
  val predTbl = appProp.getProperty("receiver.hbase.whl_pred.table")

  def createReceiverContext1(): StreamingContext = {
    
    // If you do not see this printed, that means the StreamingContext has been loaded
    // from the new checkpoint
    System.out.println(System.currentTimeMillis() + "::DBG::WildDetectorApp::createReceiverContext:: Creating new context with below parameters ")
    System.out.println(System.currentTimeMillis() + "principal=" + principal + "#keytab="+ keyTab + "#duration=" + duration + "checkpointPath=" + checkpointPath + "#hdfsPath=" + hdfsPath + "#errPath=" + errPath + "#fileMsgPath=" + fileMsgPath + "#hbaseTbl=" + hbaseTbl + "#mvgAvgTbl=" + mvgAvgTbl + "#predTbl=" + predTbl + "#runMode=" + runMode)

    var hbaseWrite: Int = FAILURE
    var hdfsWrite: Int = FAILURE

    var ssc : StreamingContext = null
    var msgDStream : ReceiverInputDStream[String] = null
    
    try {
      
      //Set up Kerberos UGI
      if (UserGroupInformation.isSecurityEnabled) {
        if (principal == null || keyTab == null) {
          throw new Error(" You have to specify keytab and principal when security is enabled ")
        }
      }
      
      if (UserGroupInformation.isLoginKeytabBased()) {
         UserGroupInformation.getLoginUser().reloginFromKeytab();
      } else if (UserGroupInformation.isLoginTicketBased()) {
         UserGroupInformation.getLoginUser().reloginFromTicketCache();
      }

      // Create the context with second batch size
      ssc = new StreamingContext(sc, Seconds(duration))
      
      SQLContext.setActive(sqlContext)
      
      ssc.checkpoint(checkpointPath)

      val receiver = new WildMessageReceiver()
      msgDStream = ssc.receiverStream(receiver)
      // val unionStream = ssc.union(msgDStream)
    
      System.out.println(System.currentTimeMillis() + "::DBG::createReceiverContext::principal=" + principal + "#keytab="+ keyTab + "#duration=" + duration + "checkpointPath=" + checkpointPath + "#hdfsPath=" + hdfsPath + "#errPath=" + errPath + "#fileMsgPath=" + fileMsgPath + "#hbaseTbl=" + hbaseTbl + "#mvgAvgTbl=" + mvgAvgTbl + "#predTbl=" + predTbl + "#runMode=" + runMode)
     hbaseWrite = FAILURE

     //Write the message to HBase
     hbaseWrite = WriteToHBase(msgDStream, hbaseTbl, hdfsPath, errPath, fileMsgPath, mvgAvgTbl, predTbl, principal, keyTab)

     System.out.println(System.currentTimeMillis() + "::DBG::createReceiverContext Write to HBASE OK-NOT OK" + hbaseWrite)

    } catch {
      case ex: Exception => {
        System.out.println(System.currentTimeMillis() + "::DBG::createReceiverContext::Exception caught in outer block..stop if both HBase/HDFS action failed..")
        ex.printStackTrace()
        throw ex
      }
    }

    ssc

  }

  //def createContext(duration: Long, checkpointDirectory: String, hdfsPath: String, errPath: String, hbaseTbl: String): StreamingContext = 
  def createReceiverContext(): StreamingContext = {
    
    // If you do not see this printed, that means the StreamingContext has been loaded
    // from the new checkpoint
    System.out.println(System.currentTimeMillis() + "::DBG::WildDetectorApp::createReceiverContext:: Creating new context with below parameters ")
    System.out.println(System.currentTimeMillis() + "principal=" + principal + "#keytab="+ keyTab + "#duration=" + duration + "checkpointPath=" + checkpointPath + "#hdfsPath=" + hdfsPath + "#errPath=" + errPath + "#fileMsgPath=" + fileMsgPath + "#hbaseTbl=" + hbaseTbl + "#mvgAvgTbl=" + mvgAvgTbl + "#predTbl=" + predTbl + "#runMode=" + runMode)

    var hbaseWrite: Int = FAILURE
    var hdfsWrite: Int = FAILURE

    var ssc : StreamingContext = null
    var msgDStream : ReceiverInputDStream[String] = null
    
    try {
      
      // Create the context with second batch size
      ssc = new StreamingContext(sc, Seconds(duration))
      
      SQLContext.setActive(sqlContext)
      
      ssc.checkpoint(checkpointPath)

      if (UserGroupInformation.isLoginKeytabBased()) {
         UserGroupInformation.getLoginUser().reloginFromKeytab();
      } else if (UserGroupInformation.isLoginTicketBased()) {
         UserGroupInformation.getLoginUser().reloginFromTicketCache();
      }

      val receiver = new WildMessageReceiver()
      msgDStream = ssc.receiverStream(receiver)
      // val unionStream = ssc.union(msgDStream)
    
      System.out.println(System.currentTimeMillis() + "::DBG::createReceiverContext::principal=" + principal + "#keytab="+ keyTab + "#duration=" + duration + "checkpointPath=" + checkpointPath + "#hdfsPath=" + hdfsPath + "#errPath=" + errPath + "#fileMsgPath=" + fileMsgPath + "#hbaseTbl=" + hbaseTbl + "#mvgAvgTbl=" + mvgAvgTbl + "#predTbl=" + predTbl + "#runMode=" + runMode)
     hbaseWrite = FAILURE

     //Write the message to HBase
     hbaseWrite = WriteToHBase(msgDStream, hbaseTbl, hdfsPath, errPath, fileMsgPath, mvgAvgTbl, predTbl, principal, keyTab)

     System.out.println(System.currentTimeMillis() + "::DBG::createReceiverContext Write to HBASE OK-NOT OK" + hbaseWrite)

    } catch {
      case ex: Exception => {
        System.out.println(System.currentTimeMillis() + "::DBG::createReceiverContext::Exception caught in outer block..stop if both HBase/HDFS action failed..")
        ex.printStackTrace()
        throw ex
      }
    }

    ssc

  }

  def createFileContext() : StreamingContext = {

    // If you do not see this printed, that means the StreamingContext has been loaded
    // from the new checkpoint
    System.out.println(System.currentTimeMillis() + "::DBG::WildDetectorApp::createFileContext:: Creating new context with below parameters ")
    System.out.println(System.currentTimeMillis() + "principal=" + principal + "#keytab="+ keyTab + "#duration=" + duration + "checkpointPath=" + checkpointPath + "#hdfsPath=" + hdfsPath + "#errPath=" + errPath + "#fileMsgPath=" + fileMsgPath + "#hbaseTbl=" + hbaseTbl + "#mvgAvgTbl=" + mvgAvgTbl + "#predTbl=" + predTbl + "#runMode=" + runMode)

    var hbaseWrite: Int = FAILURE

    var ssc : StreamingContext = null
    var msgDStream : DStream[String] = null

    try {

      //Set up Kerberos UGI
      if (UserGroupInformation.isSecurityEnabled) {
        if (principal == null || keyTab == null) {
          throw new Error(" You have to specify keytab and principal when security is enabled ")
        }
      }
      
      if (UserGroupInformation.isLoginKeytabBased()) {
         UserGroupInformation.getLoginUser().reloginFromKeytab();
      } else if (UserGroupInformation.isLoginTicketBased()) {
         UserGroupInformation.getLoginUser().reloginFromTicketCache();
      }

      // Create the context with second batch size
      ssc = new StreamingContext(sc, Seconds(duration))

      SQLContext.setActive(sqlContext)

      ssc.checkpoint(checkpointPath)

      val DELIMITER = "\u0001"

      val tmpMsgDStream = ssc.textFileStream(fileMsgPath)

      System.out.println(System.currentTimeMillis() + "::DBG::createFileContext PRE MAP ")
      msgDStream = tmpMsgDStream.map(record => record.split("\u0001")(7))
      msgDStream.print()
      System.out.println(System.currentTimeMillis() + "::DBG::createFileContext POST MAP ")

      // val unionStream = ssc.union(msgDStream)
          
      System.out.println(System.currentTimeMillis() + "::DBG::createFileContext::principal=" + principal + "#keytab="+ keyTab + "#duration=" + duration + "checkpointPath=" + checkpointPath + "#hdfsPath=" + hdfsPath + "#errPath=" + errPath + "#fileMsgPath=" + fileMsgPath + "#hbaseTbl=" + hbaseTbl + "#mvgAvgTbl=" + mvgAvgTbl + "#predTbl=" + predTbl + "#runMode=" + runMode)
      hbaseWrite = FAILURE

      //Write the message to HBase
      hbaseWrite = WriteToHBase(msgDStream, hbaseTbl, hdfsPath, errPath, fileMsgPath, mvgAvgTbl, predTbl, principal, keyTab)

      //Process Algo
      // System.out.println(System.currentTimeMillis() + "::DBG::createFileContext Calc Algo " + hbaseWrite)
      // processAlgo(msgDStream, mvgAvgTbl, predTbl)
            
    } catch {
      case ex: Exception => {
        System.out.println(System.currentTimeMillis() + "::DBG::createFileContext::Exception caught in outer block..stop if both HBase/HDFS action failed..")
        ex.printStackTrace()
        throw ex
      }
    }

    ssc
  
  }
  
  def createNewSSC(): StreamingContext = {
    
    var ssc : StreamingContext = null
    
    val SOURCE_AS_QUEUE: Int = 1
    val SOURCE_AS_FILES: Int = 2

    if (runMode == SOURCE_AS_FILES){
      ssc = createFileContext()
    }// else  //Default 
      ssc = createReceiverContext()
      
    ssc
  }

}

