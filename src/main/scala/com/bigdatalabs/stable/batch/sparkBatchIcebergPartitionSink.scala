package com.bigdatalabs.stable.batch

import com.bigdatalabs.stable.utils.{configGenerator, prepareSQL, schemaGenerator}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object sparkBatchIcebergPartitionSink {

  def main(args: Array[String]): Unit = {

    //Variables
    var _configParams: Map[String, String] = null

    var _srcFileName: String = null
    var _srcSchemaFile: String = null
    var _fileFormat: String = null
    var _inferSchemaFlag: String = null
    var _headerFlag: String = null
    var _delimiter: String = null

    var _dbName: String = null
    var _tgtTblName: String = null

    var _partitionCol: String = null //Comma Sep partition Columns in Correct Order
    var _partitionColSeq: Seq[String] = null //Sequence of partition Columns in Correct Order

    //SQL Block
    var _SQLFilePath: String = null
    var _SQLStmt: String = null

    //Session
    val spark = SparkSession.builder
      .appName("spark-batch-iceberg-sink-with-partition")
      .master("local[*]")
      .getOrCreate()

    //Set Logging Level
    spark.sparkContext.setLogLevel("WARN")

    if (args.length < 1) {
      System.err.println(
        s"""
           |Usage: --resource file <file path>
                """.stripMargin)
      System.exit(1)
    }

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
    _partitionCol = _configParams("partitionCol")

    _SQLFilePath = _configParams("SQLFilePath")

    println("=======================================================================\n")
    println("SPARK SERVICE NAME: " + this.getClass.getName.toUpperCase().dropRight(1))
    print("=========================================================================\n")
    println("RESOURCE FILE: " + _prop_file_path)
    print("=========================================================================\n")
    println("SQL FILE: " + _SQLFilePath)
    print("=========================================================================\n")
    println("SCHEMA FILE: " + _srcSchemaFile)
    print("SERVICE PARAMETERS:======================================================\n")
    println("dbName : " + _dbName)
    println("tgtTblName : " + _tgtTblName)
    println("partitionCol : " + _partitionCol)
    print("=========================================================================\n")

    //Generate Schema
    val _srcSchema: StructType = new schemaGenerator().getStruct(_srcSchemaFile)

    //Check Schema
    if (_srcSchema == null) {
      System.out.println("Schema Undefined - Exiting")
      System.exit(4)
    }

    //Fetch SQL
    _SQLStmt = new prepareSQL().getSQLStatement(_SQLFilePath)

    if (_SQLStmt == null) {
      println("Undefined SQL - Exiting")
      System.exit(4)
    }

    //Partition Column Set
    _partitionColSeq = _partitionCol.split(",").toSeq

    if (_partitionColSeq == null) {
      println("Partition Columns Not Defined - Exiting")
      System.exit(4)
    }


    //Read from Files
    spark.read.format(_fileFormat)
      .schema(schema = _srcSchema)
      .option("inferSchema", _inferSchemaFlag)
      .option("header", _headerFlag)
      .option("sep", _delimiter)
      .load(_srcFileName)
      .toDF().createOrReplaceTempView(viewName = "genericTempView")

    val df_tgt = spark.sql(_SQLStmt)

    //Debug
    //System.out.println(df_tgt.show(10))
    //System.exit(100)

    //Determine Partition Columns
    val _partColumns = _partitionColSeq
    val _partColumnNames = _partColumns.map(_colName => col(_colName))

    try {
      println(s"""Start Loading Iceberg""")
      //Append new Data
      df_tgt
        .sortWithinPartitions(_partColumnNames: _*)
        .writeTo(_dbName + "." + _tgtTblName)
        .append()

      println("SRC COUNT :" + df_tgt.count().toString)

      println(s"""Loading Completed""")

    }
    catch {
      case ex: Exception =>

        System.out.println(s"""Loading Failed""")
        System.out.println(ex.printStackTrace())

    } finally {
      spark.stop()
    }
  }
}