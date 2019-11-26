package com.bnsf.customreceiver

//import com.bnsf.model.ioc._
//import com.bnsf.model.ioc.Constants

//import com.bnsf.model.ioc.Message
//import com.bnsf.model.ioc.Header

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

// class WildMessageReceiver(val propFileName: String) extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) {

// class WildMessageReceiver extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) { //_2 Not required when WAL enabled
class WildMessageReceiver extends Receiver[String](StorageLevel.MEMORY_AND_DISK) {

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

      // val propFileName = "/config/config.properties"
      // SOP("In readProperties without params cfg file=" + propFileName)
      // val inputStream: InputStream = this.getClass.getResourceAsStream(propFileName)
      // appProp = new Properties()
      // appProp.load(inputStream)

      val propFileName = "wild.properties"
      val inputStream: FileInputStream = new FileInputStream(propFileName)
      appProp = new Properties()
      appProp.load(inputStream)
      inputStream.close();

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
        System.out.println(System.currentTimeMillis() + "::DBG::IOException reading Properties file..restarting" + io.getMessage)
        io.printStackTrace()
        throw io
      }
      case ex: Exception => {
        System.out.println(System.currentTimeMillis() + "::DBG::Exception reading Properties file data..restarting" + ex.getMessage)
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

    // SOP("Using JNDI to read objects from a foreign naming/directory service sample.")
    // SOP("Using server: " + foreignProviderURL)

    var factory: javax.jms.ConnectionFactory = null
    var destination: Destination = null

    var msgList = ArrayBuffer[javax.jms.Message]()
    var txtMsgList = ArrayBuffer[String]()
    var msg: javax.jms.Message = null

    var NUM_MESSAGES : Int = appProp.getProperty("receiver.hdfs.msgs.count").toInt //assuming each message is 5MB avg
    var MSG_TIMEOUT_SEC : Int = appProp.getProperty("receiver.hdfs.msgs.timeout.sec").toInt //default 10s , assuming message rate of 280-300/hr 

    if (foreignProviderURL.substring(0, 5) == "ldap:") usingLDAP = true

    try {

      System.out.println(System.currentTimeMillis() + "::DBG Factory=" + foreignProviderFactory + "#URL" + foreignProviderURL + "#JNDILookUp=" + connectionFactoryName + "#TOPIC_QUEUE=" + destinationName + "#Usr=" + userName + "#pass=" + password)

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

      if (factory.isInstanceOf[javax.jms.ConnectionFactory] && destination != null) {
        val ackMode = com.tibco.tibjms.Tibjms.EXPLICIT_CLIENT_ACKNOWLEDGE //Session.CLIENT_ACKNOWLEDGE
        val cf: javax.jms.ConnectionFactory = factory.asInstanceOf[javax.jms.ConnectionFactory]
        connectionRecv = cf.createConnection(userName, password)
        sessionRecv = connectionRecv.createSession(ackMode)
        msgConsumer = sessionRecv.createConsumer(destination)

        // mapper object created on each executor node
        val mapper = new ObjectMapper() // with ScalaObjectMapper
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        mapper.registerModule(DefaultScalaModule)

        connectionRecv.start()

        var counter : Int = 0

        breakable {

          //Check if we should stop receiving any more messages or continue
          while (!isStopped()) {

            //Once NUM_MESSAGES are collected, store
            //This would also ensure not too many files are created
            //File size when written to HDFS is good amount [15*5 ~ 100-200MB] 

            // System.out.println(System.currentTimeMillis() + "::DBG::WildMessageReceiver::receiver msgList.size" + msgList.size + "#NUM_MESSAGES=" + NUM_MESSAGES)
            if (counter == NUM_MESSAGES) {

              System.out.println(System.currentTimeMillis() + "::DBG::WildMessageReceiver::receiver MATCHED msgList.size == NUM_MESSAGES =" + NUM_MESSAGES)
              msgList.foreach { item =>
                val txt = item.asInstanceOf[TextMessage]
//              System.out.println(System.currentTimeMillis() + "::DBG::WildMessageReceiver::receiver In loop msgList " + txt.getText.substring(1,50))

                txtMsgList.add(txt.getText)
              }
              store(txtMsgList)

              System.out.println(System.currentTimeMillis() + "::DBG::WildMessageReceiver::receiver after store ")
              //Acknowledge message receipt after returning from store call 
              msgList.foreach { ackMsg =>
                ackMsg.acknowledge()
              }

              //Clean buffers
              txtMsgList.clear()
              msgList.clear()

              //Reset the counter
              counter = 0

            } //End if size==num_msgs

            // Receive messages from queue if available, if not wait till timeout and return null
            msg = msgConsumer.receive(MSG_TIMEOUT_SEC*1000)

            // Receive messages if available immediately and do not block
            // msg = msgConsumer.receiveNoWait() //TODO to test if it is still able to reliably receive all messages
            if (msg == null) {
              // stop("Stopping receiving messages from TIBCO..")
              System.out.println(System.currentTimeMillis() + "::DBG::Possible timeout due to NO messages in TIBCO queue..will retry after " + MSG_TIMEOUT_SEC + " secs")
              Thread.sleep(MSG_TIMEOUT_SEC)
            }else {
  
              //check validity of message or trigger exception to stop receiver 
              //From Header get MsgId and Detector type
  
              val txt = msg.asInstanceOf[TextMessage]
              var message: JsonNode = mapper.readTree(txt.getText).path("header")
              var msgId: Long = message.path("messageId").asLong()
  
              System.out.println(System.currentTimeMillis() + "#BNSF msgID" + msgId + "#JMS Props@corrID=" + msg.getJMSCorrelationID() + "#msgID=" + msg.getJMSMessageID() + "#msgType=" + msg.getJMSType() + "#msgDelvTimestamp=" + msg.getJMSDeliveryTime() + "#msgTimestamp=" + msg.getJMSTimestamp()) //                + "#="
              //FOR TESTING PURPOSES throw new JMSException("Forcing error for testing..")
  
              //Add to buffer as we are calling store(LIST) call which will block till replication is done
              //to ensure reliability
  
              msgList.add(msg)
  
              //Increment counter
              counter = counter + 1

              if (counter == NUM_MESSAGES) {
  
                System.out.println(System.currentTimeMillis() + "::DBG::WildMessageReceiver::receiver MATCHED msgList.size == NUM_MESSAGES =" + NUM_MESSAGES)
                msgList.foreach { item =>
                  val txt = item.asInstanceOf[TextMessage]
  //              System.out.println(System.currentTimeMillis() + "::DBG::WildMessageReceiver::receiver In loop msgList " + txt.getText.substring(1,50))
  
                  txtMsgList.add(txt.getText)
                }
                store(txtMsgList)
  
                System.out.println(System.currentTimeMillis() + "::DBG::WildMessageReceiver::receiver after store ")
                //Acknowledge message receipt after returning from store call 
                msgList.foreach { ackMsg =>
                  ackMsg.acknowledge()
                }
  
                //Clean buffers
                txtMsgList.clear()
                msgList.clear()
  
                //Reset the counter
                counter = 0
  
              } //End if size==num_msgs

            } //msg == null

          } 

        } //Breakable

        //If messages exist, clear them up before STOP
        if (counter > 0) {

          System.out.println(System.currentTimeMillis() + "::DBG::WildMessageReceiver::receive STOP File found, processing msgs in current batch [cnt=" + counter + "]")
          msgList.foreach { item =>
            val txt = item.asInstanceOf[TextMessage]
//          System.out.println(System.currentTimeMillis() + "::DBG::WildMessageReceiver::receive In loop msgList " + txt.getText.substring(1,50))

            txtMsgList.add(txt.getText)
          }
          store(txtMsgList)

          System.out.println(System.currentTimeMillis() + "::DBG::WildMessageReceiver::receiver after store ")
          //Acknowledge message receipt after returning from store call 
          msgList.foreach { ackMsg =>
            ackMsg.acknowledge()
          }

          //Clean buffers
          txtMsgList.clear()
          msgList.clear()

          //Reset the counter
          counter = 0

        } //End if size==num_msgs

        // If control is here, then stop file was found
        System.out.println(System.currentTimeMillis() + "::DBG::WildMessageReceiver::receive Found RECEIVER STOP file, initiating Receiver shutdown to stop receiving messages from TIBCO..sleeping for 120 s")
        Thread.sleep(120)
        stop(System.currentTimeMillis() + "::DBG::WildMessageReceiver::receive Stopping receiving messages from TIBCO..")

      } //if destn != null

    } catch {
      case e: JMSException => {
        System.out.println(System.currentTimeMillis() + "::DBG::JMSException receiving data..stopping Receiver" + e.getMessage)
        e.printStackTrace()
        reportError("JMSException receiving data..stopping Receiver", e)
        stop("Stopping receiving messages from TIBCO due to above error..")
        throw e
      }
      case e: NamingException => {
        System.out.println(System.currentTimeMillis() + "::DBG::NamingException receiving data..stopping Receiver" + e.getMessage)
        e.printStackTrace()
        reportError("Stopping receiving messages from TIBCO due to above error..", e)
        stop("Stopping receiving messages from TIBCO due to above error..")
        //restart("NamingException receiving data..", e)
        throw e
      }
      case t: Throwable => {
        System.out.println(System.currentTimeMillis() + "::DBG::General Error receiving data..stopping Receiver" + t.getMessage)
        t.printStackTrace()
        reportError("General Error receiving data..stopping Receiver", t)
        stop( "General Error receiving data ..stopping Receiver")
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

  var receiver: Thread = null

  def onStart() {

    if (!isStopped()) {

      receiver = new Thread() {

        override def run() {
          // while(! isStopped() ) {
          System.out.println(System.currentTimeMillis() + "::DBG::Calling receive in thread..")
          receive()
          //}
        }
      }

      receiver.start()

    }

  }

  def onStop() {

    System.out.println(System.currentTimeMillis() + "::DBG::WildMessageReceiver::onStop Stopping the Receiver")

    try {

      if (connectionRecv != null)
        connectionRecv.close()

      if (sessionRecv != null)
        sessionRecv.close()

      if (msgConsumer != null)
        msgConsumer.close()

      receiver.interrupt()
      //Sleep for 60s to allow flush of messages
      receiver = null
      Thread.sleep(60)
      System.out.println(System.currentTimeMillis() + "::DBG::WildMessageReceiver::opStop Stopped the Receiver thread!! ")

    } catch {
      case ex: JMSException => {
        System.out.println(System.currentTimeMillis() + "::DBG::WildMessageReceiver::onStop JMSException in stop() " + ex.getMessage)
        ex.printStackTrace()
        throw ex
      }
    }

    System.out.println(System.currentTimeMillis() + "::DBG::WildMessageReceiver::onStop closed all connections!!")

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
