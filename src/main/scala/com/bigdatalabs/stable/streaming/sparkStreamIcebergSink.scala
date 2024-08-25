/*

Author : Anand
Description : Streaming Data Sink for Iceberg
Date : 17/10/2023

*/

package com.bigdatalabs.stable.streaming

import com.bigdatalabs.stable.utils.{configGenerator, schemaGenerator}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._

import java.util.concurrent.TimeUnit

object sparkStreamIcebergSink {

    def main(args: Array[String]): Unit = {

        //Variables
        var _configParams: Map[String, String] = null

        var _brokers: String = null
        var _subsTopic: String = null
        var _srcSchemaFile: String = null
        var _offSet: String = null
        var _triggerDurationMinutes: Int = 0

        var _checkPointLocation: String = null

        var _dbName: String = null
        var _tgtTblName: String = null

        //Session
        val spark = SparkSession.builder
          .master(master="local[*]")
          .appName(name="spark-streaming-iceberg-sink")
          .getOrCreate()

        //Set Logging Level
        spark.sparkContext.setLogLevel("WARN")

        //Fetch Property File Path from Input Parameter
        val _prop_file_path: String = args(0)

        if (_prop_file_path == null) {
            println("Property File Not Found - Exiting")
            System.exit(4)
        }

        //Check for Properties File
        _configParams = new configGenerator().getParams(_prop_file_path)

        if (_configParams == null) {
            println("Check Configuration File - Exiting")
            System.exit(1)
        }

        _brokers = _configParams("brokers")
        _subsTopic = _configParams("subsTopic")
        _offSet = _configParams("offSet")
        _triggerDurationMinutes = _configParams("triggerDurationMins").toInt
        _checkPointLocation = _configParams("checkPointLocation") + this.getClass.getName.dropRight(1)+ "-" + (System.currentTimeMillis() / 1000)

        _srcSchemaFile = _configParams("srcSchemaFile")

        _dbName = _configParams("dbName")
        _tgtTblName = _configParams("tgtTblName")

        print("SERVICE PARAMETERS==================================================\n")
        println("brokers :" + _brokers)
        println("subsTopic :" + _subsTopic)
        println("offSet :" + _offSet)
        println("trigger Duration :" + _triggerDurationMinutes + " Minutes")

        println("srcSchemaFile :" + _srcSchemaFile)
        println("dbName :" + _dbName)
        println("tgtTblName :" + _tgtTblName)
        print("====================================================================\n")

        //Generate Schema
        val _msgSchema: StructType = new schemaGenerator().getSchema(_srcSchemaFile)

        if (_msgSchema == null) {
            println("Undefined Schema - Exiting")
            System.exit(3)
        }

        try {
            val df_value = spark.readStream
              .format(source="kafka")
              .option("kafka.bootstrap.servers", _brokers)
              .option("subscribe", _subsTopic)
              .option("startingOffsets", _offSet)
              .option("failOnDataLoss", value = false)
              .load()

            //Read Value and Convert to String
            val df_streaming = df_value.selectExpr("CAST(value AS STRING)")

            val df_tgt = df_streaming
              .select(from_json(col("value"), _msgSchema)
                .as(alias="data"))
              .select(col="data.*")

            df_tgt.writeStream
              .format(source="iceberg")
              .outputMode(outputMode="append")
              .trigger(Trigger.ProcessingTime(_triggerDurationMinutes, TimeUnit.MINUTES))
              .option("checkpointLocation", _checkPointLocation)
              .toTable(_dbName + "." + _tgtTblName)
              .awaitTermination()

        } catch {
            case ex: Exception =>
                println(ex.printStackTrace())
        } finally {
            spark.stop()
        }
    }
}