/*
* Author : Anand
* Date : 17-Oct-2023
* Description: Spark Batch Sink for Iceberg
*/

package com.bigdatalabs.stable.batch

import com.bigdatalabs.stable.utils.generateSchema
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import java.io.{FileNotFoundException, IOException}
import scala.io.{BufferedSource, Source}

object sparkBatchIcebergSink {

    def main(args: Array[String]): Unit = {

        //Variables
        var _configFile: BufferedSource = null

        var _srcFileName: String = null
        var _srcSchemaFile: String = null
        var _fileFormat: String = null
        var _inferSchemaFlag: String = null
        var _headerFlag: String = null
        var _delimiter: String = null
        var _quoteChar: String = null

        var _dbName: String = null
        var _tgtTblName: String = null
        var _partitionCol: String = null

        var _SQL: String = null

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
        val _prop_file_path = args(0)

        //Check for Properties File
        try {
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

        //Parameters
        _srcFileName = _configMap("srcFileName")
        _fileFormat = _configMap("fileFormat")
        _srcSchemaFile = _configMap("srcSchemaFile")

        _inferSchemaFlag = _configMap("inferSchemaFlag")
        _headerFlag = _configMap("headerFlag")
        _delimiter = _configMap("delimiter")
        _quoteChar = _configMap("quoteChar")

        _dbName = _configMap("dbName")
        _tgtTblName = _configMap("tgtTblName")
        _partitionCol = _configMap("partitionCol")

        _SQL = _configMap("SQL")

        //Generate Schema
        val _srcSchema: StructType = new generateSchema().getStruct(_srcSchemaFile)

        //Check Schema
        if (_srcSchema == null) {
            System.out.println("Schema Undefined - Exiting")
            System.exit(3)
        }

        //_srcSchema.printTreeString()

        //Read from Files
        val df_raw = spark.read.format(_fileFormat)
          .schema(_srcSchema)
          .option("inferSchema", _inferSchemaFlag)
          .option("header", _headerFlag)
          .option("sep", _delimiter)
          .option("quote", _quoteChar)
          .load(_srcFileName)
          .toDF()

        //df_raw.printSchema()
        System.out.println("Loading Data to Target")

        df_raw.createOrReplaceTempView("genericTempTable")

        val _DML =
            s"""
               |SELECT station,CAST(rep_date as date) ,CAST(latitude as double),CAST(longitude as double),name,CAST(temp as double)
               |FROM stg_station
               |""".stripMargin

        val df_station = spark.sql(_SQL.stripMargin)

        try {
            println(s"Start Loading Iceberg")
            //Append new Data
            df_station
              //              .sortWithinPartitions("station")
              .writeTo(_dbName + "." + _tgtTblName)
              .append()

            println("SRC COUNT :" + df_station.count().toString)
            println(s"Loading Completed")
        }
        catch {
            case ex: Exception =>
                System.out.println("Loading Failed")
                System.out.println(ex.printStackTrace())

        } finally {
            println("SRC COUNT :" + df_station.count().toString)
            println(s"Loading Completed")
            spark.stop()
        }
    }
}
