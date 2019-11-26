#!/bin/bash
export SPARK_MAJOR_VERSION=2
spark-submit --class o_iqp_order_booking_fact --master yarn --num-executors 2   \
    --driver-memory 30g   --executor-memory 30g --executor-cores 5 --conf spark.scheduler.mode=FAIR --conf spark.driver.cores=1 \
    --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:/home/rajeebp/log4j.xml" \
    --conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=file:/home/rajeebp/log4j.xml" \
    --jars /home/rajeebp/pgCommonTest/target/scala-2.11/utils_2.11-1.0.jar \
    /home/rajeebp/iqp_fact/target/scala-2.11/iqp-fact_2.11-1.0.jar
echo "Spark job completed"
