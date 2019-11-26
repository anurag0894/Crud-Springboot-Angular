package com.bnsf.customreceiver

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

object SQLHiveContextSingleton {
    @transient private var instance: HiveContext = _
    def getInstance(sparkContext: SparkContext): HiveContext = {
        synchronized {
              if (instance == null ) {
                instance = new HiveContext(sparkContext)
            }
            instance
        }
    }
}

