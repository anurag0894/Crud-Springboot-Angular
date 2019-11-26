package com.bnsf.customreceiver

import org.apache.spark.sql.{SQLContext, _}
import org.apache.spark.sql.execution.datasources.hbase._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.fs.Path
import org.apache.hadoop.security.UserGroupInformation
import java.security.PrivilegedExceptionAction
import org.apache.hadoop.hbase.client.ConnectionFactory

import scala.util.Properties

case class HBaseRecord(
    col0: String,
    col1: Boolean,
    col2: Double,
    col3: Float)


object HBaseRecord {
  def apply(i: Int): HBaseRecord = {
    val s = s"""row${"%03d".format(i)}"""
    HBaseRecord(s,
      i % 2 == 0,
      i.toDouble,
      i.toFloat)
  }
}

object DetectorHBaseCatalogs {

                // |"table":{"namespace":"techm", "name":"shcExampleTable1"},
                // |"table":{"namespace":"default", "name":"shcExampleTable1"},
  val cat = s"""{
                |"table":{"name":"techm:shcExampleTable1"},
                |"rowkey":"key",
                |"columns":{
                |"col0":{"cf":"rowkey", "col":"key", "type":"string"},
                |"col1":{"cf":"cf1", "col":"col1", "type":"boolean"},
                |"col2":{"cf":"cf2", "col":"col2", "type":"double"},
                |"col3":{"cf":"cf3", "col":"col3", "type":"float"}
                |}
                |}""".stripMargin

  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName("HBaseTest").set("hbase.security.authentication", "kerberos").set("hbase.security.authorization", "true")

  val conf = HBaseConfiguration.create()
    conf.addResource(new Path("/etc/hbase/conf/hbase-site.xml"))
    conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"))
    conf.set("hbase.client.keyvalue.maxsize", "0")
    conf.set("hbase.rpc.controllerfactory.class","org.apache.hadoop.hbase.ipc.RpcControllerFactory")
    conf.set("hadoop.security.authentication", "kerberos")
    conf.set("hbase.security.authentication", "kerberos")

    Properties.setProp("java.security.krb5.conf", "/etc/krb5.conf")
    Properties.setProp("sun.security.krb5.debug", "true")

    // val principal = "hdetladm@RAILS.RWY.BNSF.COM"
    // val keyTab="/home/hdetladm/hdetladm.keytab"
    // val keyTab="/home/hdetladm/hdetladm.headless.keytab"
    // val keyTab="/etc/security/keytabs/hdetladm.headless.keytab"

    val principal = "hdetldev@RAILS.RWY.BNSF.COM"
    val keyTab="/etc/security/keytabs/hdetldev.headless.keytab"
    UserGroupInformation.setConfiguration(conf)

    var connection : org.apache.hadoop.hbase.client.Connection = null

    // val ugi=UserGroupInformation.loginUserFromKeytab(principal, keyTab)
    val ugi=UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keyTab)
    UserGroupInformation.setLoginUser(ugi)

    ugi.doAs(new PrivilegedExceptionAction[Unit] {
      def run: Unit = {
	connection = ConnectionFactory.createConnection(conf)
        println("DBG: Past run connection init..")
      }
    })

    val sc = new SparkContext(sparkConf)
    // val sqlContext = new SQLContext(sc)
    val sqlContext = new HiveContext(sc)

    println("DBG: Past sparkSqlContext init ..")
    import sqlContext.implicits._

    def withCatalog(cat: String): DataFrame = {
      sqlContext
        .read
        .options(Map(HBaseTableCatalog.tableCatalog->cat))
        .format("org.apache.spark.sql.execution.datasources.hbase")
        .load()
    }

    val data = (16 to 20).map { i =>
      HBaseRecord(i)
    }

    println("DBG: before parallelize write ..")
    // for testing connection sharing only
    sc.parallelize(data).toDF.write.options(
      Map(HBaseTableCatalog.tableCatalog -> cat, HBaseTableCatalog.newTable -> "1"))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()
    println("DBG: post Write ..")

    val df = withCatalog(cat)
    println("DBG: pre SHOW .." + df)

    df.take(10)
    df.filter($"col0" <= "row006")
      .select($"col0", $"col1").show
    println("DBG: post SHOW ..")
    df.filter($"col0" === "row005" || $"col0" <= "row006")
      .select($"col0", $"col1").show
    df.filter($"col0" > "row250")
      .select($"col0", $"col1").show

    df.registerTempTable("table1")
    val c = sqlContext.sql("select count(col1) from table1 where col0 < 'row050'")
    c.show()
    val d = sqlContext.sql("select * from table1 where col0 < 'row050'")
    d.show()
    println("DBG: post COUNT ..")

    sc.stop()
  }
}
