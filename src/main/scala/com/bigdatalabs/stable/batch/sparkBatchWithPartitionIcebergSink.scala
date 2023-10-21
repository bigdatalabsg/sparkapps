package com.bigdatalabs.stable.batch

import com.bigdatalabs.stable.utils.generateSchema
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._


import java.io.{FileNotFoundException, IOException}
import scala.io.{BufferedSource, Source}

object sparkBatchWithPartitionIcebergSink {

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
        var _tgtSchemaFile: String =null

        var _dbName: String = null
        var _tgtTblName: String = null
        var _partitionColumnSeq: Seq [String]   = null //Sequence of partition Columns in Correct Order

        var _SQL: String = null

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

        _tgtSchemaFile = _configMap("tgtSchemaFile")
        _dbName = _configMap("dbName")
        _tgtTblName = _configMap("tgtTblName")

        //Partition Column Set
        _partitionColumnSeq = _configMap("partitionColumnSeq").split(",").toSeq

        if(_partitionColumnSeq==null) {
            println("Partition Columns Not Defined - Exiting")
            System.exit(3)
        }

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
        val df_src= spark.read.format(_fileFormat)
          .schema(_srcSchema)
          .option("inferSchema", _inferSchemaFlag)
          .option("header", _headerFlag)
          .option("sep", _delimiter)
          .option("quote", _quoteChar)
          .load(_srcFileName)
          .toDF()

        //df_raw.printSchema()
        System.out.println("Loading Data to Target")

        //Create Temp table
        df_src.cache()
        df_src.createOrReplaceTempView(viewName="genericTempTable")

        val df_tgt = spark.sql(_SQL.stripMargin)

        //Determine Partition Columns
        val _partColumns = _partitionColumnSeq
        val _partColumnNames = _partColumns.map(_colName=>col(_colName))

        try {
            println(s"""Start Loading Iceberg""")
            //Append new Data
            df_tgt
              .sortWithinPartitions(_partColumnNames:_*)
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
            println("SRC COUNT :" + df_tgt.count().toString)
            println(s"""Loading Completed""")
            spark.stop()
        }
    }
}