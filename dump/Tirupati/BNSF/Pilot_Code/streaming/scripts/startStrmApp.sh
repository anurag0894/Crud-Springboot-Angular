#!/bin/bash

debug() 
{
  #echo "[${USER}][`date "+%F_%T"`] - ${*}" >> ${LOG_FILE}
  echo "[DBG `date "+%Y%m%d_%H%M%S"`] - ${*}" 
}

sendEmail()
{
  mail -s "Streaming Application has reported an issue, please look into it !!" $MAIL_ID
}

MAIL_ID=TECHSVCSDLBigDataAnalyticsSupport@BNSF.com
INTERVAL=600

CURR_DIR=$(dirname $0)/..
echo "Running from $CURR_DIR"

export HADOOP_CONF_DIR=/etc/hadoop/conf:/etc/hive/conf:/etc/hbase/conf
export YARN_CONF_DIR=/etc/hadoop/conf:/etc/hive/conf:/etc/hbase/conf

export APP_SPARK_CLASSPATH=/usr/hdp/current/hbase-client/lib/hbase-common.jar,/usr/hdp/current/hbase-client/lib/hbase-client.jar,/usr/hdp/current/hbase-client/lib/hbase-server.jar,/usr/hdp/current/hbase-client/lib/hbase-protocol.jar,/usr/hdp/current/hbase-client/lib/guava-12.0.1.jar,/usr/hdp/current/spark-client/lib/datanucleus-api-jdo-3.2.6.jar,/usr/hdp/current/spark-client/lib/datanucleus-core-3.2.10.jar,/usr/hdp/current/spark-client/lib/datanucleus-rdbms-3.2.9.jar,/usr/hdp/current/spark-client/lib/spark-assembly-1.6.2.2.5.0.2-3-hadoop2.7.3.2.5.0.2-3.jar
export SPARK_CLASSPATH=/usr/hdp/current/hbase-client/lib/hbase-common.jar:/usr/hdp/current/hbase-client/lib/hbase-client.jar:/usr/hdp/current/hbase-client/lib/hbase-server.jar:/usr/hdp/current/hbase-client/lib/hbase-protocol.jar:/usr/hdp/current/hbase-client/lib/guava-12.0.1.jar:/etc/hbase/conf/hbase-site.xml:/usr/hdp/current/spark-client/lib/datanucleus-api-jdo-3.2.6.jar:/usr/hdp/current/spark-client/lib/datanucleus-core-3.2.10.jar:/usr/hdp/current/spark-client/lib/datanucleus-rdbms-3.2.9.jar:/usr/hdp/current/spark-client/lib/spark-assembly-1.6.2.2.5.0.2-3-hadoop2.7.3.2.5.0.2-3.jar:/etc/hadoop/conf

hdfs dfs -rmr -skipTrash /bnsf/hd/mechanical/prep/ioc/detector/stop/*

while true
do

  debug "Starting Streaming Appln from $CURR_DIR.. removing any stop files on /bnsf/hd/mechanical/prep/ioc/detector/stop/"

#WORKING CMD
#kinit -kt /etc/security/keytabs/hdetldev.headless.keytab hdetldev@RAILS.RWY.BNSF.COM
	#--principal bihdpipelndv@RAILS.RWY.BNSF.COM --keytab /home/bihdpipelndv/bihdpipelndv.keytab \
/usr/bin/spark-submit \
	--conf spark.yarn.maxAppAttempts=2 \
	--conf spark.yarn.am.attemptFailuresValidityInterval=1h \
	--conf spark.yarn.max.executor.failures=2 \
	--conf spark.task.maxFailures=2 \
	--conf spark.speculation=false \
	--conf spark.hadoop.fs.hdfs.impl.disable.cache=true \
	--master yarn-cluster \
	--class com.bnsf.customreceiver.WildDetectorMain \
	--jars $APP_SPARK_CLASSPATH \
	--files $CURR_DIR/config/wild.properties,$CURR_DIR/config/hbase-site.xml,/etc/hadoop/conf/core-site.xml,$CURR_DIR/config/hive-site.xml \
	--num-executors 4 \
	--driver-memory 1g \
	--executor-memory 2g --executor-cores 4 \
	--properties-file $CURR_DIR/config/application.conf \
	$CURR_DIR/lib/data-streaming.jar

  hdfs dfs -ls /bnsf/hd/mechanical/prep/ioc/detector/stop/*
  if [ $? -eq 0 ]
  then
    debug "Exiting without re-attempt as found STOP file on /bnsf/hd/mechanical/prep/ioc/detector/stop/ " 
    debug "Stopping Streaming app without re-attempt as found STOP file on /bnsf/hd/mechanical/prep/ioc/detector/stop/ " | sendEmail
    exit 0
  else
    debug "Error processing " | sendEmail
  fi

  debug "Sleeping for $INTERVAL seconds, before making next attempt to start.."
  sleep $INTERVAL

done
