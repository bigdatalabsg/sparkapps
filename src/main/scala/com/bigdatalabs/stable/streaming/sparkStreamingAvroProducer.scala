package com.bigdatalabs.stable.streaming

/*
* Author : Anand
* Date : 15-Oct-2023
* Description: Structured Streaming
*/

import com.bigdatalabs.stable.utils.generateSchema
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.avro.functions._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import java.io.{FileNotFoundException, IOException}
import scala.io.{BufferedSource, Source}

object sparkStreamingAvroProducer {

    def main(args: Array[String]): Unit = {

        var _configFile: BufferedSource = null

        var _brokers: String = null
        var _subsTopic: String = null
        var _srcSchemaFile: String = null
        var _avroSchemaFile: String = null
        var _tgtSchemaFile: String = null
        var _pubsTopic: String = null
        var _offSet: String = null
        var _groupId: String = null
        var _microbatchSecs: Int = 0
        var _streamFormat: String = null
        var _lineSplitterChar: String = null
        var _quoteChar: String = null
        var _delimiterChar: String = null

        //Spark Context
        val spark = SparkSession.builder()
          .master(master = "local[*]")
          .appName(name = "spark structured streaming")
          .getOrCreate()

        //Set Logging Level
        spark.sparkContext.setLogLevel("WARN")

        //Fetch Property File Path from Input Parameter
        val _prop_file_path = args(0)

        //Check for Properties File
        try {
            print("=======================================================================\n")
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
        _srcSchemaFile = _configMap("srcSchemaFile")
        _avroSchemaFile = _configMap("avroSchemaFile")
        _tgtSchemaFile = _configMap("tgtSchemaFile")
        _pubsTopic = _configMap("pubsTopic")
        _offSet = _configMap("offSet")
        _streamFormat = _configMap("fileFormat")
        _groupId = _configMap("groupId")
        _microbatchSecs = _configMap("microbatchSecs").toInt
        _lineSplitterChar = _configMap("lineSplitterChar")
        _delimiterChar = _configMap("delimiterChar")
        _quoteChar = _configMap("quoteChar")

        println("brokers ::" + _brokers)
        println("subsTopic ::" + _subsTopic)
        println("srcSchemaFile ::" + _srcSchemaFile)
        println("avroSchemaFile ::" + _avroSchemaFile)
        println("tgtSchemaFile ::" + _tgtSchemaFile)
        println("pubsTopic ::" + _pubsTopic)
        println("offSet ::" + _offSet)
        println("groupId ::" + _groupId)
        println("microbatchSecs ::" + _microbatchSecs)
        println("lineSplitterChar ::" + _lineSplitterChar)
        println("delimiterChar ::" + _delimiterChar)
        println("quoteChar ::" + _quoteChar)

        //Generate Schema
        val _srcSchema: StructType = new generateSchema().getStruct(_srcSchemaFile)

        if (_srcSchema == null) {
            println("Bad Schema - Exiting")
            System.exit(3)
        }

        //Read from JSON Message Stream
        val df_stream = spark
          .readStream
          .format("kafka")
          .option("kafka.bootstrap.servers", _brokers)
          .option("subscribe", _subsTopic)
          .option("startingOffsets", _offSet) // From starting
          .option("mode", "PERMISSIVE")
          //.option("failOnDataLoss", "false")
          .load()

        //Extract Value from Kafka Key Value and cast to String
        val df_value = df_stream.selectExpr("CAST(value AS STRING)")

        //Apply Schema and Resolve to columns
        val df_subs = df_value.select(
            from_json(
                col(colName = "value"), _srcSchema
            ).as(alias = "data")
        )

        //inspect Schema
        //df_subs.printSchema()

        //Convert to Avro
        val df_avro = df_subs.select(
            to_avro(
                struct(colName = "data.*")
            ) as "value"
        )

        //df_avro.printSchema()

        val _checkPointLocation = "/tmp/spark_kafka_chkpnt/" +
          "streaming/" + this.getClass.getName + (System.currentTimeMillis() / 1000)

        try {
            //Publish to avro source
            df_avro.writeStream
              .format(source = "kafka")
              .option("kafka.bootstrap.servers", _brokers)
              .option("topic", _pubsTopic)
              .outputMode(outputMode = "append")
              .option("checkpointLocation", _checkPointLocation)
              //.option("failOnDataLoss", "false")
              .start()
              .awaitTermination()

            //// For Debug
            /*
            df_avro.writeStream
              .format("console")
              .outputMode("append")
              .option("truncate", false)
              .start
              .awaitTermination()
            */

        } catch {
            case ex: Exception =>
                println(ex.printStackTrace())
        } finally {
            spark.stop()
        }

    }
}