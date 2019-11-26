#!/bin/bash

export HADOOP_CONF_DIR=/etc/hadoop/conf
#/etc/hbase/conf
export YARN_CONF_DIR=/etc/hadoop/conf
#:/etc/hbase/conf
export SPARK_CLASSPATH=/usr/hdp/current/hbase-client/lib/hbase-common.jar:/usr/hdp/current/hbase-client/lib/hbase-client.jar:/usr/hdp/current/hbase-client/lib/hbase-server.jar:/usr/hdp/current/hbase-client/lib/hbase-protocol.jar:/usr/hdp/current/hbase-client/lib/guava-12.0.1.jar:/etc/hbase/conf/hbase-site.xml

#/usr/bin/spark-submit \
#  --class com.bnsf.customreceiver.WildMessageReceiver \
#  --master yarn \
#  --deploy-mode cluster \ 
#  --executor-memory 1G \
#  --num-executors 1 \
#  /bnsf/sandbox/techm/app/wild-detector/build/libs/wild-detector-1.0.0.jar


#Working command
#/usr/bin/spark-submit --class com.bnsf.customreceiver.WildMessageReceiverHDP --master yarn --deploy-mode cluster --executor-memory 1G --properties-file ./config/application.conf --files /usr/hdp/current/hbase-client/conf/hbase-site.xml /bnsf/sandbox/techm/app/myTest/build/libs/myTest-1.0.0.jar
#java -cp build/libs/myTest-1.0.0.jar testJSONParse
#exit 1
#WORKING CMD
#--queue tst_queue \
/usr/bin/spark-submit \
	--conf spark.yarn.maxAppAttempts=4 \
	--conf spark.yarn.am.attemptFailuresValidityInterval=1h \
	--conf spark.yarn.max.executor.failures=3 \
	--conf spark.yarn.executor.failuresValidityInterval=1h \
	--conf spark.task.maxFailures=8 \
	--class com.bnsf.customreceiver.WildMessageReceiverHDP \
	--master yarn-cluster \
	--jars /usr/hdp/current/hbase-client/lib/hbase-common.jar,/usr/hdp/current/hbase-client/lib/hbase-client.jar,/usr/hdp/current/hbase-client/lib/hbase-server.jar,/usr/hdp/current/hbase-client/lib/hbase-protocol.jar,/usr/hdp/current/hbase-client/lib/guava-12.0.1.jar,/usr/hdp/current/hbase-client/lib/htrace-core-3.1.0-incubating.jar \
	--files /etc/hbase/conf/hbase-site.xml \
	--num-executors 2 \
	--driver-memory 512m \
	--executor-memory 512m --executor-cores 2 \
	--conf spark.speculation=true \
	--properties-file ./config/application.conf \
	--principal hdetladm@RAILS.RWY.BNSF.COM --keytab /etc/security/keytabs/hdetladm.headless.keytab      \
	--conf spark.hadoop.fs.hdfs.impl.disable.cache=true \
	build/libs/myTest-1.0.0.jar 

#/usr/bin/spark-submit --class com.bnsf.customreceiver.DetectorHBaseCatalogs --master yarn-cluster --jars /usr/hdp/current/hbase-client/lib/hbase-common.jar,/usr/hdp/current/hbase-client/lib/hbase-client.jar,/usr/hdp/current/hbase-client/lib/hbase-server.jar,/usr/hdp/current/hbase-client/lib/hbase-protocol.jar,/usr/hdp/current/hbase-client/lib/guava-12.0.1.jar,/usr/hdp/current/hbase-client/lib/htrace-core-3.1.0-incubating.jar --files /etc/hbase/conf/hbase-site.xml --num-executors 2 --driver-memory 512m --executor-memory 512m --executor-cores 2 build/libs/myTest-1.0.0.jar --properties-file ./config/application.conf 

#/usr/bin/spark-submit --class com.bnsf.customreceiver.WildMessageReceiverHDPHDFS --master yarn-cluster --jars /usr/hdp/current/hbase-client/lib/hbase-common.jar,/usr/hdp/current/hbase-client/lib/hbase-client.jar,/usr/hdp/current/hbase-client/lib/hbase-server.jar,/usr/hdp/current/hbase-client/lib/hbase-protocol.jar,/usr/hdp/current/hbase-client/lib/guava-12.0.1.jar,/usr/hdp/current/hbase-client/lib/htrace-core-3.1.0-incubating.jar --files /etc/hbase/conf/hbase-site.xml --num-executors 2 --driver-memory 512m --executor-memory 512m --executor-cores 2 build/libs/myTest-1.0.0.jar --properties-file ./config/application.conf --principal hdetladm@RAILS.RWY.BNSF.COM --keytab /etc/security/keytabs/hdetladm.headless.keytab

#Try with client for hbase
#/usr/bin/spark-submit --class com.bnsf.customreceiver.WildMessageReceiverHDP --master yarn --deploy-mode client --executor-memory 1G --properties-file ./config/application.conf --files /usr/hdp/current/hbase-client/conf/hbase-site.xml /bnsf/sandbox/techm/app/myTest/build/libs/myTest-1.0.0.jar


#/usr/bin/spark-submit --class org.apache.spark.examples.streaming.CustomReceiver --master yarn --deploy-mode cluster --executor-memory 1G --properties-file ./config/application.conf /bnsf/sandbox/techm/app/myTest/build/libs/myTest-1.0.0.jar toplxhded002 9999

#/bnsf/sandbox/techm/app/myTest/build/libs/wild-detector-1.0.0.jar 

#To Run HBASE
export SPARK_CLASSPATH=/usr/hdp/current/hbase-client/lib/hbase-common.jar:/usr/hdp/current/hbase-client/lib/hbase-client.jar:/usr/hdp/current/hbase-client/lib/hbase-server.jar:/usr/hdp/current/hbase-client/lib/hbase-protocol.jar:/usr/hdp/current/hbase-client/lib/guava-12.0.1.jar:/etc/hbase/conf/hbase-site.xml

#export HADOOP_OPTS="-Dsun.security.krb5.debug=true"

#/usr/bin/spark-submit --conf "spark.executor.extraJavaOptions= -Djava.security.auth.login.config=/usr/hdp/current/hbase-client/conf/hbase_client_jaas.conf" --conf "spark.driver.extraJavaOptions= -Djava.security.auth.login.config=/usr/hdp/current/hbase-client/conf/hbase_client_jaas.conf" --class com.bnsf.customreceiver.DetectorHBaseCatalogs --master yarn-client --properties-file ./config/application.conf --files /usr/hdp/current/hbase-client/conf/hbase-site.xml build/libs/myTest-1.0.0.jar

#/usr/bin/spark-submit --verbose --class com.bnsf.customreceiver.DetectorHBaseCatalogs --master yarn-cluster --properties-file ./config/application.conf --files /usr/hdp/current/hbase-client/conf/hbase-site.xml,/bnsf/sandbox/techm/app/myTest/config/core-site.xml --conf "spark.executor.extraJavaOptions= -Djava.security.auth.login.config=/usr/hdp/current/hbase-client/conf/hbase_client_jaas.conf -Dsun.security.krb5.debug=true" --conf "spark.driver.extraJavaOptions= -Djava.security.auth.login.config=/usr/hdp/current/hbase-client/conf/hbase_client_jaas.conf -Dsun.security.krb5.debug=true" build/libs/myTest-1.0.0.jar

#/usr/bin/spark-submit --class com.bnsf.customreceiver.DetectorHBaseCatalogs --master yarn-cluster --properties-file ./config/application.conf --files /usr/hdp/current/hbase-client/conf/hbase-site.xml,/bnsf/sandbox/techm/app/myTest/config/core-site.xml build/libs/myTest-1.0.0.jar

#WORKING
#/usr/bin/spark-submit --class com.bnsf.customreceiver.Main --master yarn-client --jars /usr/hdp/current/hbase-client/lib/hbase-common.jar,/usr/hdp/current/hbase-client/lib/hbase-client.jar,/usr/hdp/current/hbase-client/lib/hbase-server.jar,/usr/hdp/current/hbase-client/lib/hbase-protocol.jar,/usr/hdp/current/hbase-client/lib/guava-12.0.1.jar,/usr/hdp/current/hbase-client/lib/htrace-core-3.1.0-incubating.jar --files /etc/hbase/conf/hbase-site.xml --num-executors 2 --driver-memory 512m --executor-memory 512m --executor-cores 1 build/libs/myTest-1.0.0.jar --properties-file ./config/application.conf 
#WORKING
#/usr/bin/spark-submit --class com.bnsf.customreceiver.DetectorHBaseCatalogs --master yarn-cluster --jars /usr/hdp/current/hbase-client/lib/hbase-common.jar,/usr/hdp/current/hbase-client/lib/hbase-client.jar,/usr/hdp/current/hbase-client/lib/hbase-server.jar,/usr/hdp/current/hbase-client/lib/hbase-protocol.jar,/usr/hdp/current/hbase-client/lib/guava-12.0.1.jar,/usr/hdp/current/hbase-client/lib/htrace-core-3.1.0-incubating.jar --files /etc/hbase/conf/hbase-site.xml --num-executors 2 --driver-memory 512m --executor-memory 512m --executor-cores 2 build/libs/myTest-1.0.0.jar --properties-file ./config/application.conf 

