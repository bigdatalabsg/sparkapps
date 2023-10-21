package com.bigdatalabs.stable.streaming

/*
* Author : Anand
* Date : 15-Oct-2023
* Description: Generic Streaming Data Template for Microbatch
*/

import com.bigdatalabs.stable.utils.generateSchema

import scala.io.{BufferedSource, Source}
import java.io.{FileNotFoundException, IOException}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object sparkStreamingMicroBatch extends Serializable {

    def main(args: Array[String]): Unit = {

        //Variables

        var _configFile: BufferedSource = null

        var _brokers: String = null
        var _subsTopic: String = null
        var _msgSchemaFile: String = null
        var _groupId: String = null
        var _microbatchSecs: Int = 0
        var _streamFormat: String = null
        var _lineSplitterChar: String = null
        var _quoteChar: String = null
        var _delimiterChar: String = null

        //Spark Context
        val spark = SparkSession.builder()
          .master("local[*]")
          .appName("kafka stream handler")
          .getOrCreate()

        //Set Logging Level
        spark.sparkContext.setLogLevel("WARN")

        //Fetch Property File Path from Input Parameter
        val _prop_file_path = args(0)

        //Check for Properties File
        try {
            println("=======================================================================\n")
            println("SPARK SERVICE NAME:" + this.getClass.getName.toUpperCase())
            print("=======================================================================\n")
            println("RESOURCE FILE:" + _prop_file_path)
            print("=======================================================================\n")

            _configFile = Source.fromFile(_prop_file_path)

        } catch {
            case ex: FileNotFoundException =>
                println(ex.printStackTrace())
                System.exit(1)

            case ex: IOException =>
                println(ex.printStackTrace())
                System.exit(2)

        }

        //Read Application Config
        val _configMap = _configFile.getLines().filter(line => line.contains("::")).map { line =>
            val _configTokens = line.split("::")
            if (_configTokens.size == 1) {
                _configTokens(0) -> ""
            } else {
                _configTokens(0) -> _configTokens(1)
            }
        }.toMap

        //Initialize Variables
        _brokers = _configMap("brokers")
        _subsTopic = _configMap("subsTopic")
        _msgSchemaFile = _configMap("msgSchemaFile")
        _streamFormat = _configMap("fileFormat")
        _groupId = _configMap("groupId")
        _microbatchSecs = _configMap("microbatchSecs").toInt
        _lineSplitterChar = _configMap("lineSplitterChar")
        _delimiterChar = _configMap("delimiterChar")
        _quoteChar = _configMap("quoteChar")

        //WHY?
        val topicsSet : Set[String] = _subsTopic.split(",").toSet
        //Set Streaming Context and Latency
        val ssc = new StreamingContext(spark.sparkContext, Seconds(_microbatchSecs))

        //Kafka Params
        val kafkaParams = Map[String, Object](
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> _brokers,
            ConsumerConfig.GROUP_ID_CONFIG -> _groupId,
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer]
        )

        //Read from Kafka
        val _readStream = KafkaUtils.createDirectStream[String, String](ssc, LocationStrategies.PreferConsistent,
            ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams))

        //Generate Schema from Schema Generation Class
        val _msgSchema: StructType = new generateSchema().getStruct(_msgSchemaFile)

        if (_msgSchema == null) {
            System.out.println("Bad Schema - Exiting")
            System.exit(3)
        }

        try {
            //Read the Value Pair, Ignore Key
            val _processStream = _readStream.map(line => line.value)
            //Process Record
            _processStream.foreachRDD(
                _rddRecord => {
                    import spark.implicits._

                    val _rawDF = _rddRecord.toDF("results")
                    val df_results = _rawDF.select(from_json($"results", _msgSchema) as "data").select("data.*")
                    df_results.cache()
                    //df_results.printSchema()
                    print(df_results.show(false))
                })

            // Start the computation
            ssc.start()
            ssc.awaitTermination()

        } catch {
            case ex: Exception => ex.printStackTrace()
        } finally {
            ssc.stop(stopSparkContext = true, stopGracefully = true)
        }
    }
}