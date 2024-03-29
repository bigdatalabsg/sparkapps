package com.bigdatalabs.stable.streaming

/*
* Author : Anand
* Date : 17-Oct-2023
* Description: Structured Streaming AVRO Consumer
*/

import com.bigdatalabs.stable.utils.generateSchema
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._

import java.io.{FileNotFoundException, IOException}
import java.util.concurrent.TimeUnit
import scala.io.{BufferedSource, Source}

object sparkStreamPartitionIcebergSink {

    def main(args: Array[String]): Unit = {

        //Variables
        var _configFile: BufferedSource = null

        var _brokers: String = null
        var _subsTopic: String = null
        var _srcSchemaFile: String = null
        var _offSet: String = null
        var _triggerDurationMinutes: Int = 0

        var _dbName: String = null
        var _tgtTblName: String = null
        var _partitionCol: String = null

        var _SQL: String = null

        //Session
        val spark = SparkSession.builder
          .master("local[*]")
          .appName("spark-streaming-iceberg-sink-partition")
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

        _brokers = _configMap("brokers")
        _subsTopic = _configMap("subsTopic")
        _offSet = _configMap("offSet")
        _triggerDurationMinutes = _configMap("triggerDurationMinutes").toInt

        _srcSchemaFile = _configMap("srcSchemaFile")
        _dbName = _configMap("dbName")
        _tgtTblName = _configMap("tgtTblName")
        _partitionCol = _configMap("partitionCol")

        print("SERVICE PARAMETERS==================================================\n")
        println("brokers :" + _brokers)
        println("subsTopic :" + _subsTopic)
        println("offSet :" + _offSet)
        println("trigger Duration :" + _triggerDurationMinutes + " Minutes")

        println("srcSchemaFile :" + _srcSchemaFile)
        println("dbName :" + _dbName)
        println("tgtTblName :" + _tgtTblName)
        println("partitionCol :" + _partitionCol)

        print("====================================================================\n")

        //Generate Schema
        val _msgSchema: StructType = new generateSchema().getStruct(_srcSchemaFile)

        if (_msgSchema == null) {
            println("Undefined Schema - Exiting")
            System.exit(4)
        }

        try {

            val df_value = spark.readStream
              .format("kafka")
              .option("kafka.bootstrap.servers", _brokers)
              .option("subscribe", _subsTopic)
              .option("startingOffsets", _offSet)
              .load()

            //Read Value and Convert to String
            val df_streaming = df_value.selectExpr("CAST(value AS STRING)")

            val df_tgt = df_streaming
              .select(from_json(col("value"), _msgSchema)
                .as("data")).select("data.*")

            val _checkPointLocation = "/tmp/spark_kafka_chkpnt/" +
              "streaming/" + this.getClass.getName + (System.currentTimeMillis() / 1000)

            df_tgt.writeStream
              .format("iceberg")
              .outputMode("append")
              .trigger(Trigger.ProcessingTime(_triggerDurationMinutes, TimeUnit.MINUTES))
              .option("checkpointLocation", _checkPointLocation)
              .option("fanout-enabled", "true")
              .toTable(_dbName + "." + _tgtTblName)
              .awaitTermination

        } catch {
            case ex: Exception =>
                println(ex.printStackTrace())
        } finally {
            spark.stop()
        }
    }
}