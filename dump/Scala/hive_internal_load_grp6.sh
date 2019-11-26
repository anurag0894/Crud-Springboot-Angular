#!/bin/bash
export SPARK_MAJOR_VERSION=2
spark-submit --class Ingestion --master yarn  --num-executors 2 --driver-memory 3g  --executor-memory 8g --executor-cores 4 --conf spark.scheduler.mode=FAIR  --conf spark.driver.cores=1  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer  /home/rajeebp/FinalCode/target/scala-2.11/simple-project_2.11-1.0.jar filter=group_number=6 truncate=true
echo "hive load group 6 loaded successfully"
