package com.bigdatalabs.stable.streaming

/*
* Author : Anand
* Date : 17-Oct-2023
* Description: Structured Streaming AVRO Consumer
*/

import com.bigdatalabs.stable.utils.{configGenerator, preparedStatementGenerator, schemaGenerator}

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._

import java.util.concurrent.TimeUnit

object sparkStreamIcebergPartitionSink {

  def main(args: Array[String]): Unit = {

    //Variables
    var _configParams: Map[String, String] = null
    var _checkPointLocation: String = null

    var _brokers: String = null
    var _subsTopic: String = null
    var _srcSchemaFile: String = null
    var _offSet: String = null
    var _triggerDurationMinutes: Int = 0

    var _dbName: String = null
    var _tgtTblName: String = null
    var _partitionCol: String = null
    var _partitionColSeq: Seq[String] = null

    //SQL Block
    var _SQLFilePath: String = null
    var _SQLStmt: String = null

    //Session
    val spark: SparkSession = SparkSession.builder
      .master("local")
      .appName("spark-streaming-iceberg-sink-partition")
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
    _triggerDurationMinutes = _configParams("triggerDurationMinutes").toInt

    _srcSchemaFile = _configParams("srcSchemaFile")
    _SQLFilePath = _configParams("SQLFilePath")

    _dbName = _configParams("dbName")
    _tgtTblName = _configParams("tgtTblName")
    _partitionCol = _configParams("partitionCol")

    _checkPointLocation = _configParams("checkPointLocation") + this.getClass.getName.dropRight(1) + "-" + (System.currentTimeMillis() / 1000)

    print("=============================================================================================================\n")
    println("SPARK SERVICE NAME:" + this.getClass.getName.toUpperCase().dropRight(1))
    print("=============================================================================================================\n")
    println("RESOURCE FILE:" + _prop_file_path)
    print("=============================================================================================================\n")
    println("SQL FILE:" + _SQLFilePath)
    print("=============================================================================================================\n")
    println("SCHEMA FILE :" + _srcSchemaFile)
    print("=============================================================================================================\n")
    println("CHECK POINT DIR :" + _checkPointLocation)
    print("============================================= SERVICE PARAMETERS ============================================\n")
    println("brokers :" + _brokers)
    println("subsTopic :" + _subsTopic)
    println("offSet :" + _offSet)
    println("trigger Duration :" + _triggerDurationMinutes + " Minutes")
    println("dbName :" + _dbName)
    println("tgtTblName :" + _tgtTblName)
    println("partitionCol :" + _partitionCol)
    print("=============================================================================================================\n")

    //Generate Source Schema
    val _msgSchema: StructType = new schemaGenerator().getSchema(_srcSchemaFile)

    if (_msgSchema == null) {
      println("Undefined Schema - Exiting")
      System.exit(10)
    }

    _SQLStmt = new preparedStatementGenerator().getStatement(_SQLFilePath)

    if (_SQLStmt == null) {
      println("Undefined SQL - Exiting")
      System.exit(10)
    }

    //Partition Column Set
    _partitionColSeq = _partitionCol.split(",").toSeq

    if (_partitionColSeq == null) {
      println("Partition Columns Not Defined - Exiting")
      System.exit(10)
    }

    try {
      //Read Stream
      val df_streaming = spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", _brokers)
        .option("subscribe", _subsTopic)
        .option("startingOffsets", _offSet)
        .option("failOnDataLoss", value = false)
        .load()

      //Read Value Column and Convert to String
      df_streaming
        .selectExpr("CAST(value AS STRING)")
        .select(from_json(col("value"), _msgSchema).as("data")).select("data.*")
        .createOrReplaceTempView(viewName = "genericTempStreamingView")

      //Target
      val df_tgt = spark.sql(_SQLStmt)

      //Write Stream
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