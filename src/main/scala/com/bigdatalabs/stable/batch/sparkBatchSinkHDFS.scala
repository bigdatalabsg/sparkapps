package com.bigdatalabs.stable.batch

/*
* Author : Anand
* Date : 15-Oct-2023
* Description: Generic Streaming Data Template for Microbatch
*/

import com.bigdatalabs.stable.utils.generateSchema
import org.apache.spark.sql.SparkSession

import java.io.{FileNotFoundException, IOException}
import scala.io.{BufferedSource, Source}

object sparkBatchSinkHDFS {

    def main(args: Array[String]): Unit = {

        //Fetch Property File Path from Input Parameter
        var _prop_file_path = args(0)
        var _configFile: BufferedSource = null

        var _srcFile: String = null
        var _schemaFile: String = null
        var _whloc: String = null
        var _thrftSrvr: String = null
        var _dbName: String = null
        var _tgtTblName: String = null
        var _fileFormat: String = null
        var _inferSchemaFlag: String = null
        var _headerFlag: String = null
        var _delimiter: String = null
        var _quoteChar: String = null

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

        val _configMap = _configFile.getLines().filter(line => line.contains("::")).map { line =>
            val _configTokens = line.split("::")
            if (_configTokens.size == 1) {
                _configTokens(0) -> ""
            } else {
                _configTokens(0) -> _configTokens(1)
            }
        }.toMap

        _srcFile = _configMap("srcFile")
        _schemaFile = _configMap("schemaFile")
        _whloc = _configMap("whloc")
        _thrftSrvr = _configMap("thrftSrvr")
        _dbName = _configMap("dbName")
        _tgtTblName = _configMap("tgtTblName")
        _fileFormat = _configMap("fileFormat")
        _inferSchemaFlag = _configMap("inferSchemaFlag")
        _headerFlag = _configMap("headerFlag")
        _delimiter = _configMap("delimiter")
        _quoteChar = _configMap("quoteChar")

        //Session
        val spark = SparkSession.builder
          .master("local[*]") //"spark://localhost:7077"
          .appName("spark file Ingestor")
          .config("hive.metastore.uris", _thrftSrvr)
          .config("spark.sql.warehouse.location", _whloc)
          .config("spark.sql.warehouse.dir", _whloc)
          .enableHiveSupport()
          .getOrCreate()

        //Generate Schema from Schema Generation Class
        val _srcSchema = new generateSchema().getStruct(_schemaFile)

        if (_srcSchema == null) {
            System.out.println("Bad Schema - Exiting")
            System.exit(3)
        }

        try {
            val df_raw = spark.read.format(_fileFormat)
              .schema(_srcSchema)
              .option("inferSchema", _inferSchemaFlag)
              .option("header", _headerFlag)
              .option("sep", _delimiter)
              .option("quote", _quoteChar)
              .load(_srcFile)
              .toDF()

            df_raw.write
              .format("orc")
              .mode("overwrite")
              .saveAsTable(_dbName + "." + _tgtTblName)

        } catch {
            case ex: Exception =>
                System.out.println(ex.printStackTrace())
                System.exit(2)

        } finally {
            spark.stop()
        }
    }
}