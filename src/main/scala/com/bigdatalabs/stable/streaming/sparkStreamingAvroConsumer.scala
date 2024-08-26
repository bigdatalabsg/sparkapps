/*
* Author : Anand
* Date : 17-Oct-2023
* Description: Structured Streaming AVRO Consumer
*/

package com.bigdatalabs.stable.streaming

import com.bigdatalabs.utils.{avroSchemaGenerator, configGenerator}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.avro.functions.from_avro
import org.apache.spark.sql.functions.col

object sparkStreamingAvroConsumer {

  def main(args: Array[String]): Unit = {

    var _configParams: Map[String, String] = null

    var _brokers: String = null
    var _subsTopic: String = null
    var _offSet: String = null
    var _srcSchemaFile: String = null
    var _avroSchemaFile: String = null
    var _tgtSchemaFile: String = null
    var _pubsTopic: String = null
    var _groupId: String = null

    //Spark Context
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("spark structured streaming")
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
    _offSet = _configParams("offSet")
    _groupId = _configParams("groupId")

    _pubsTopic = _configParams("pubsTopic")

    _srcSchemaFile = _configParams("srcSchemaFile")
    _avroSchemaFile = _configParams("avroSchemaFile")
    _tgtSchemaFile = _configParams("tgtSchemaFile")

    //Fetch AVRO schema
    //val _avroSchema:String = new String(
      //Files.readAllBytes(Paths.get(_avroSchemaFile)))

    val _avroSchema: String = new avroSchemaGenerator().returnAvroSchema(_avroSchemaFile)

    print("=============================================================================================================\n")
    println("SPARK SERVICE NAME:" + this.getClass.getName.toUpperCase().dropRight(1))
    print("=============================================================================================================\n")
    println("RESOURCE FILE:" + _prop_file_path)
    print("=============================================================================================================\n")
    println("SCHEMA FILE :" + _srcSchemaFile)
    print("============================================= SERVICE PARAMETERS ============================================\n")
    println("brokers :" + _brokers)
    println("subsTopic :" + _subsTopic)
    println("groupId :" + _groupId)
    println("offSet :" + _offSet)
    print("=============================================================================================================\n")

    //Read from JSON Message Stream
    val df_stream = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", _brokers)
      .option("subscribe", _subsTopic)
      .option("startingOffsets", _offSet) // From starting
      .option("failOnDataLoss", value = false)
      .load()

    //Inspect Schema
    //df_stream.printSchema()
    val df_from_avro = df_stream.select(
      //from_avro(col("value"), _avroSchema).alias("data")
      from_avro(col("value"), _avroSchema).alias("data")
    ).select("data.*")

    df_from_avro.writeStream
      .format("console")
      .outputMode("append")
      .option("mode", value = "PERMISSIVE")
      .option("truncate", value = false)
      .start()
      .awaitTermination()
  }
}