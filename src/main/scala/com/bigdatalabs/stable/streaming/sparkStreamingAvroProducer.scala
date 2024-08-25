package com.bigdatalabs.stable.streaming

/*
* Author : Anand
* Date : 15-Oct-2023
* Description: Structured Streaming
*/

import com.bigdatalabs.stable.utils.{configGenerator, schemaGenerator}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.avro.functions._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object sparkStreamingAvroProducer {

  def main(args: Array[String]): Unit = {

    var _configParams: Map[String, String] = null

    var _brokers: String = null
    var _subsTopic: String = null
    var _checkPointLocation: String = null
    var _srcSchemaFile: String = null
    var _avroSchemaFile: String = null
    var _tgtSchemaFile: String = null
    var _pubsTopic: String = null
    var _offSet: String = null
    var _groupId: String = null

    //Spark Context
    val spark = SparkSession.builder()
      .master(master = "local[*]")
      .appName(name = "spark structured streaming")
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

    //Initialize Variables
    _brokers = _configParams("brokers")
    _subsTopic = _configParams("subsTopic")
    _checkPointLocation = _configParams("checkPointLocation") + this.getClass.getName.dropRight(1)+ "-" + (System.currentTimeMillis() / 1000)
    _groupId = _configParams("groupId")

    _srcSchemaFile = _configParams("srcSchemaFile")
    _avroSchemaFile = _configParams("avroSchemaFile")
    _tgtSchemaFile = _configParams("tgtSchemaFile")

    _pubsTopic = _configParams("pubsTopic")
    _offSet = _configParams("offSet")

      print("=============================================================================================================\n")
      println("SPARK SERVICE NAME:" + this.getClass.getName.toUpperCase().dropRight(1))
      print("=============================================================================================================\n")
      println("RESOURCE FILE:" + _prop_file_path)
      print("=============================================================================================================\n")
      println("SCHEMA FILE :" + _srcSchemaFile)
      print("=============================================================================================================\n")
      println("CHECK POINT DIR :" + _checkPointLocation)
      print("============================================= SERVICE PARAMETERS ============================================\n")
      println("brokers :" + _brokers)
      println("subsTopic :" + _subsTopic)
      println("groupId :" + _groupId)
      println("offSet :" + _offSet)
      print("=============================================================================================================\n")

    //Generate Schema
    val _srcSchema: StructType = new schemaGenerator().getSchema(_srcSchemaFile)

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
      .option("failOnDataLoss", "false")
      .load()
      .selectExpr("CAST(value AS STRING)")

    //Apply Schema and Resolve to columns
    val df_subs = df_stream.select(
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