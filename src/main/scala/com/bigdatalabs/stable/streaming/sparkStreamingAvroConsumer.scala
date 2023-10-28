/*
* Author : Anand
* Date : 17-Oct-2023
* Description: Structured Streaming AVRO Consumer
*/

package com.bigdatalabs.stable.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.avro.functions.from_avro
import org.apache.spark.sql.functions.col

import java.io.{FileNotFoundException, IOException}
import java.nio.file.{Files, Paths}
import scala.io.{BufferedSource, Source}

object sparkStreamingAvroConsumer {

    def main(args: Array[String]): Unit = {

        var _configFile: BufferedSource = null

        var _brokers: String = null
        var _subsTopic: String = null
        var _offSet: String =null
        var _srcSchemaFile: String = null
        var _avroSchemaFile: String = null
        var _tgtSchemaFile: String = null
        var _pubsTopic: String = null
        var _groupId: String = null
        var _microbatchSecs: Int = 0
        var _streamFormat: String = null
        var _lineSplitterChar: String = null
        var _quoteChar: String = null
        var _delimiterChar: String = null

        //Spark Context
        val spark = SparkSession.builder()
          .master("local[*]")
          .appName("spark structured streaming")
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
        _offSet = _configMap("offSet")
        _srcSchemaFile = _configMap("srcSchemaFile")
        _avroSchemaFile = _configMap("avroSchemaFile")
        _tgtSchemaFile = _configMap("tgtSchemaFile")
        _pubsTopic = _configMap("pubsTopic")
        _streamFormat = _configMap("fileFormat")
        _groupId = _configMap("groupId")
        _microbatchSecs = _configMap("microbatchSecs").toInt
        _lineSplitterChar = _configMap("lineSplitterChar")
        _delimiterChar = _configMap("delimiterChar")
        _quoteChar = _configMap("quoteChar")

        println("brokers :" + _brokers)
        println("subsTopic :" + _subsTopic)
        println("srcSchemaFile :" + _srcSchemaFile)
        println("avroSchemaFile :" + _avroSchemaFile)
        println("tgtSchemaFile :" + _tgtSchemaFile)
        println("pubsTopic :" + _pubsTopic)
        println("groupId :" + _groupId)
        println("microbatchSecs :" + _microbatchSecs)
        println("lineSplitterChar :" + _lineSplitterChar)
        println("delimiterChar :" + _delimiterChar)
        println("quoteChar :" + _quoteChar)

        //Read from JSON Message Stream
        val df_stream = spark
          .readStream
          .format("kafka")
          .option("kafka.bootstrap.servers", _brokers)
          .option("subscribe", _subsTopic)
          .option("startingOffsets", _offSet) // From starting
          .load()

        //Inspect Schema
        //df_stream.printSchema()

        //Pull Schema
        val _avroSchema = new String(
            Files.readAllBytes(Paths.get(_avroSchemaFile)))

        //
        val df_from_avro = df_stream.select(
            from_avro(col("value"),_avroSchema).alias("data")
        ).select("data.*")

        df_from_avro.writeStream
          .format("console")
          .outputMode("append")
          .option("mode", value="PERMISSIVE")
          .option("truncate", value = false)
          .start()
          .awaitTermination()
    }
}