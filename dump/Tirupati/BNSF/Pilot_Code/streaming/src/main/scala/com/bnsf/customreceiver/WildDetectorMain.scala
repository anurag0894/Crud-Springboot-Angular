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
import org.apache.spark.sql.types.{ StructType, StructField, StringType, DoubleType }

import java.io.BufferedReader
import java.io.InputStreamReader
import java.io.InputStream
import java.io.IOException
import java.io.FileInputStream

import java.util.{ Hashtable, Properties }

object WildDetectorMain {

  val SOURCE_AS_QUEUE: Int = 1
  val SOURCE_AS_FILES: Int = 2

  var runMode: Int = SOURCE_AS_QUEUE

  //Read up properties 
  var appProp: Properties = new Properties()

  // val propFileName = "/config/config.properties"
  // val inputStream: InputStream = this.getClass.getResourceAsStream(propFileName)
  // appProp.load(inputStream)

  val propFileName = "wild.properties"
  val inputStream: FileInputStream = new FileInputStream(propFileName)
  appProp.load(inputStream)
  inputStream.close()

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

  // check every 10s for shutdown hdfs file
  val checkIntervalMillis = 10000

  def main(args: Array[String]) {

    if (args.length == 1) {

      if (args(0).equalsIgnoreCase("FILE") == false) {
        System.err.println("Usage: WildDetectorApp -text")
        System.exit(1)
      } else {
        System.out.println("DBG::WildDetectorMain::main Running in File Mode ")
        runMode = SOURCE_AS_FILES
      }
    }

    try {

      val sparkConf = new SparkConf().setAppName("WILD-Detector")
      val sc = new SparkContext(sparkConf)
      val sqlContext = new HiveContext(sc)

      sc.hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive","true")

      System.out.println("DBG::WildDetectorMain::main Parameters:duration=" + duration + "#chkPt=" + checkpointPath + 
				"#hdfsPath=" + hdfsPath + "#errPath=" + errPath + 
				"#principal=" + principal + "#keyTab=" + keyTab + 
				"#hbaseTbl=" + hbaseTbl + "#mvgAvgTbl=" + mvgAvgTbl + "#predTbl=" + predTbl + 
				"#runMode=" + runMode)
     /* 
      val wildReceiver = new WildDetectorApp (sc, sqlContext, duration, checkpointPath, 
                                      hdfsPath, errPath, 
                                      principal, keyTab, 
                                      hbaseTbl, mvgAvgTbl, predTbl,
                                      runMode)
     */
      val wildReceiver = new WildDetectorApp (sc, sqlContext, runMode)
      
      val ssc = StreamingContext.getActiveOrCreate(checkpointPath, wildReceiver.createNewSSC)
      //Start the streaming context
      ssc.start()

      //Wait till application terminates on call of stop()
      if (ssc != null) {
        ssc.awaitTermination()
        System.out.println("DBG In main after awaitTermination() ")
      }

      //Shutdown gracefully
      shutDownApp(ssc)

      //TODO test before Shutdown hook in case of forced failure of Driver
      sys.ShutdownHookThread {
        //Will come to this in case of force kill
        System.out.println("DBG In Sys.ShutdownHook")
        shutDownApp(ssc)
      }

    } catch {
      case ex: Exception => {
        System.out.println("DBG In main Caught Final Exception, reason: " + ex.getMessage)
        ex.printStackTrace()
        throw ex
      }
    }

  }

  def shutDownApp(ssc: StreamingContext) {
    System.out.println("DBG Gracefully stopping the Application")
    val stopSparkContext = true
    val stopGracefully = true
    ssc.stop(stopSparkContext, stopGracefully)
  }

}
	// scalastyle:on println
