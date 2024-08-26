/*
* Author : Anand
* Date : 17-Oct-2023
* Description: Spark Batch Sink for Iceberg
*/

package com.bigdatalabs.stable.batch

import com.bigdatalabs.utils.{configGenerator, preparedStatementGenerator, schemaGenerator}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

import scala.io.Source

object sparkBatchIcebergSink {

  def main(args: Array[String]): Unit = {

    //Variables
    var _configParams: Map[String, String] = null

    var _srcFileName: String = null
    var _srcSchemaFile: String = null
    var _fileFormat: String = null
    var _inferSchemaFlag: String = null
    var _headerFlag: String = null
    var _delimiter: String = null

    //Database Block
    var _dbName: String = null
    var _tgtTblName: String = null

    //Prepared Statement Block
    var _preparedStatementFile: String=null
    var _preparedStatement:String=null

    //Session
    val spark = SparkSession.builder
      .appName("spark-batch-iceberg-sink")
      .master("local[*]")
      .getOrCreate()

    //Set Logging Level
    spark.sparkContext.setLogLevel("WARN")

    if (args.length < 1) {
      System.err.println(
        s"""
           |Usage: --resourcefile <file path>
                """.stripMargin)
      System.exit(1)
    }

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

    //Parameters
    _srcFileName = _configParams("srcFileName")
    _fileFormat = _configParams("fileFormat")
    _srcSchemaFile = _configParams("srcSchemaFile")

    _inferSchemaFlag = _configParams("inferSchemaFlag")
    _headerFlag = _configParams("headerFlag")
    _delimiter = _configParams("delimiter")

    _dbName = _configParams("dbName")
    _tgtTblName = _configParams("tgtTblName")

    _preparedStatementFile = _configParams("preparedStatementFile")

    //Check for Properties File
    print("=============================================================================================================\n")
    println("SPARK SERVICE NAME:" + this.getClass.getName.toUpperCase().dropRight(1))
    print("=============================================================================================================\n")
    println("RESOURCE FILE:" + _prop_file_path)
    print("=============================================================================================================\n")
    println("PREPARED STATEMENT FILE:" + _preparedStatementFile)
    print("=============================================================================================================\n")
    println("SCHEMA FILE :" + _srcSchemaFile)
    print("============================================= SERVICE PARAMETERS ============================================\n")
    println("dbName :" + _dbName)
    println("tgtTblName :" + _tgtTblName)
    print("=============================================================================================================\n")

    //Generate Schema
    val _srcSchema: StructType = new schemaGenerator().getSchema(_srcSchemaFile)

    //Check Schema
    if (_srcSchema == null) {
      System.out.println("Schema Undefined - Exiting")
      System.exit(4)
    }

    //Fetch Prepared Statement
    _preparedStatement = new preparedStatementGenerator().getStatement(_preparedStatementFile)

    if (_preparedStatement == null) {
      println("Undefined Prepared Statement - Exiting")
      System.exit(4)
    }

    //_srcSchema.printTreeString()

    //Read from Source
    spark.read.format(_fileFormat)
      .schema(_srcSchema)
      .option("inferSchema", _inferSchemaFlag)
      .option("header", _headerFlag)
      .option("sep", _delimiter)
      .load(_srcFileName)
      .toDF().createOrReplaceTempView("genericTempView")

    //Work Table
    val df_work = spark.sql(_preparedStatement.stripMargin)

    try {
      println(s"Start Loading Iceberg")
      //Append new Data
      df_work
        .writeTo(_dbName + "." + _tgtTblName)
        .append()

      //Validate Load
      val df_tgt = spark.table(_dbName + "." + _tgtTblName)

      println("SRC COUNT :" + df_work.count().toString)

      println("TGT COUNT :" + df_tgt.count().toString)

      println(s"Loading Completed")
    }
    catch {
      case ex: Exception =>
        System.out.println("Loading Failed")
        System.out.println(ex.printStackTrace())
    } finally {
      spark.stop()
    }
  }
}