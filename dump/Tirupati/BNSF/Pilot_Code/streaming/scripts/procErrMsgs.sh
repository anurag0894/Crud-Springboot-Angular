#!/bin/bash

export HADOOP_CONF_DIR=/etc/hadoop/conf:/etc/hbase/conf
export YARN_CONF_DIR=/etc/hadoop/conf:/etc/hbase/conf

unset SPARK_CLASSPATH
export SPARK_CLASSPATH=/usr/hdp/current/hbase-client/lib/hbase-common.jar:/usr/hdp/current/hbase-client/lib/hbase-client.jar:/usr/hdp/current/hbase-client/lib/hbase-server.jar:/usr/hdp/current/hbase-client/lib/hbase-protocol.jar:/usr/hdp/current/hbase-client/lib/guava-12.0.1.jar:/usr/hdp/current/hbase-client/lib/htrace-core-3.1.0-incubating.jar:/etc/hbase/conf/hbase-site.xml:/usr/hdp/current/spark-client/lib/datanucleus-api-jdo-3.2.6.jar:/usr/hdp/current/spark-client/lib/datanucleus-core-3.2.10.jar:/usr/hdp/current/spark-client/lib/datanucleus-rdbms-3.2.9.jar:/etc/hadoop/conf:/etc/hbase/conf

/usr/bin/spark-submit --class com.bnsf.customreceiver.WildErrMsgProcessor --master yarn-cluster --files ./config/wild.properties,/etc/hbase/conf/hbase-site.xml,./config/hive-site.xml --jars /usr/hdp/current/hbase-client/lib/hbase-common.jar,/usr/hdp/current/hbase-client/lib/hbase-client.jar,/usr/hdp/current/hbase-client/lib/hbase-server.jar,/usr/hdp/current/hbase-client/lib/hbase-protocol.jar,/usr/hdp/current/hbase-client/lib/guava-12.0.1.jar,/usr/hdp/current/hbase-client/lib/htrace-core-3.1.0-incubating.jar,/usr/hdp/current/spark-client/lib/datanucleus-api-jdo-3.2.6.jar,/usr/hdp/current/spark-client/lib/datanucleus-core-3.2.10.jar,/usr/hdp/current/spark-client/lib/datanucleus-rdbms-3.2.9.jar,/usr/hdp/current/spark-client/lib/spark-assembly-1.6.2.2.5.0.2-3-hadoop2.7.3.2.5.0.2-3.jar --num-executors 2 --driver-memory 2g --executor-memory 2g --executor-cores 3 --properties-file ./config/application.conf lib/data-streaming.jar /bnsf/hd/mechanical/raw/ioc/file/input/

#WORKING CMD 
#/usr/bin/spark-submit --class com.bnsf.customreceiver.DetectorHBaseCatalogs --master yarn-cluster --jars /usr/hdp/current/hbase-client/lib/hbase-common.jar,/usr/hdp/current/hbase-client/lib/hbase-client.jar,/usr/hdp/current/hbase-client/lib/hbase-server.jar,/usr/hdp/current/hbase-client/lib/hbase-protocol.jar,/usr/hdp/current/hbase-client/lib/guava-12.0.1.jar,/usr/hdp/current/hbase-client/lib/htrace-core-3.1.0-incubating.jar --files /etc/hbase/conf/hbase-site.xml --num-executors 2 --driver-memory 512m --executor-memory 512m --executor-cores 2 build/libs/data-streaming-1.0.0.jar --properties-file ./config/application.conf 

