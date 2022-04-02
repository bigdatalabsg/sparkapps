package com.bigdatalabs.sparkapps

import com.bigdatalabs.business.entities.cTrade


import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, concat, lit, struct, to_json}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object streamHandler extends Serializable {
  def main(args: Array[String]): Unit = {
    if (args.length < 4) {
      System.err.println(
        s"""
           |Usage: Spark Stream Handler <brokers> <groupId> <topics> <microbatch secs>
        """.stripMargin)
      System.exit(1)
    }

    //Resolve arguments
    val Array(brokers, groupId, topics, microBatchSecs) = args
    val topicsSet = topics.split(",").toSet
    val batchLatency = microBatchSecs.toInt

    //println(topicsSet)
    //println(batchLatency)

    //Spark Context
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("kafka stream handler")
      .getOrCreate()

    //Import spark
    import spark.implicits._

    //Set Logging Level
    spark.sparkContext.setLogLevel("WARN")

    //Set Streaming Context
    val ssc = new StreamingContext(spark.sparkContext, Seconds(batchLatency))

    //Kafka Params
    val kafkaParams = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer])

    //Read from Kafka
    val _readStream = KafkaUtils.createDirectStream[String, String](ssc,LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams))

    //Read the Value Pair, Ignore Key
    val _processStream = _readStream.flatMap(line=>line.value.split("/n"))

    //Process Record
    _processStream.foreachRDD(_rddRecord=>{

      val _arr = _rddRecord.collect()

      val _rdd = spark.sparkContext.parallelize(_arr).map(_.split(",")).map(_trade=>cTrade(_trade(0),_trade(1),_trade(2),_trade(3).toFloat,_trade(4).toFloat,
        _trade(5).toFloat,_trade(6).toFloat,_trade(7).toInt, _trade(8).toFloat))

      //Convert RDD to DF
      val _dfTrade =_rdd.toDF()

      print(_dfTrade.show())

      /*
      // Implement Filtering Logic
      val _dfAlert=_dfTrade
        .groupBy($"symbol")
        .agg(avg($"volume").as("avg_vol"))
        .filter($"symbol"==="AA" || $"symbol"==="ABB")
      */
    })

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }
}