package com.bigdatalabs.sparkapps

/*
import com.anrisu.sparkutilities.sparkCustomLogger
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
*/
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
/*import org.apache.spark.sql.functions*/
//import org.apache.spark.sql.functions.{col, column, explode, from_csv, from_json, map_from_arrays, split}

import org.apache.spark.sql.functions._

import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.types._
/*
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
*/
object streamHandlerNew {

  def main(args: Array[String]): Unit = {

    if (args.length < 3) {
      System.err.println(
        s"""
           |Usage: Spark Stream Handler <brokers> <groupId> <topics>
        """.stripMargin)
      System.exit(1)
    }
    //Resolve arguments
    val Array(_brokers, _groupId, _topics) = args

    // Create direct kafka stream with brokers and topics
    val _topicsSet = _topics.split(",").toSet

    val spark = SparkSession.builder()
      .appName("KafkaStreaming")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    //Define Schema
    val _tradeSchema = StructType(List(
      StructField("xchange", StringType, nullable = true),
      StructField("symbol", StringType, nullable = true),
      StructField("trate", StringType, nullable = true),
      StructField("open", DoubleType, nullable = true),
      StructField("high", DoubleType, nullable = true),
      StructField("low", DoubleType, nullable = true),
      StructField("close", DoubleType, nullable = true),
      StructField("volume", DoubleType, nullable = true),
      StructField("adj_close", DoubleType, nullable = true)
    ))

    val _parseCSV = spark.readStream
      .format("csv")
      .schema(_tradeSchema)
      .load("/home/bdluser/Projects/dataSources/stocks/streaming_source/*.csv")

    _parseCSV.printSchema()

    _parseCSV.selectExpr("to_json(struct(*)) AS value")
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", _brokers)
      .option("topic", "stock-ticker")
      .option("forceDeleteTempCheckpointLocation", "true")
      .option("checkpointLocation", "/tmp/kafka_checkpoint/new")
      .start()



    val _initDF = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "stock-ticker")
      .load()

      val df1 = _initDF.selectExpr("CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)").as[(String, Timestamp)]
      .select(from_json($"value", _tradeSchema).as("trade"), $"timestamp")
      .select("trade.*", "timestamp")

    df1.writeStream
      .format("console")
      .option("append","true")
      //.option("forceDeleteTempCheckpointLocation", "true")
      //.option("checkpointLocation", "/tmp/kafka_checkpoint/final")
      .start()
      .awaitTermination()
  }
}