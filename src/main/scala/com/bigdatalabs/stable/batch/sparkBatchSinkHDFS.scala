package com.bigdatalabs.stable.batch

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

import java.io.{FileNotFoundException, IOException}
import scala.io.Source

object sparkBatchSinkHDFS {

  def main(args: Array[String]): Unit = {

    val _whloc = "hdfs://localhost:9000/user/hive/warehouse" //warehouse location
    val _thrftSrvr = "thrift://localhost:9083" //hive thrift uri & port

    //Session
    val spark = SparkSession.builder
      .master("local[*]") //"spark://localhost:7077"
      .appName("spark file Ingestor")
      .config("hive.metastore.uris", _thrftSrvr)
      .config("spark.sql.warehouse.location", _whloc)
      .config("spark.sql.warehouse.dir", _whloc)
      .enableHiveSupport()
      .getOrCreate()

    //Fetch Property File Path from Input Parameter
    val _prop_file_path = args(0)

    //Check for Propoerties File
    try {
      print("=======================================================================\n")
      println("RESOURCE FILE:" + _prop_file_path)
      print("=======================================================================\n")
    } catch {
      case ex: FileNotFoundException => println(ex.printStackTrace())
      case ex: IOException => println(ex.printStackTrace())
    } finally {

    }

    val _configFile = Source.fromFile(_prop_file_path)

    val _configMap = _configFile.getLines().filter(line => line.contains("=")).map { line =>
      val _configTokens = line.split("=")
      if (_configTokens.size == 1) {
        _configTokens(0) -> ""
      } else {
        _configTokens(0) -> _configTokens(1)
      }
    }.toMap

    val _srcFile = _configMap("srcFile")
    val _schemaFile = _configMap("schemaFile")
    val _dbName = _configMap("dbName")
    val _tgtTblName = _configMap("tgtTblName")

    def _inferType (field: String) = field.split(":") (1) match {
      case "byte" => ByteType
      case "short" => ShortType
      case "integer" => IntegerType
      case "long" => LongType
      case "float" => FloatType
      case "double" => DoubleType
      //case "decimal" => DecimalType
      case "string" => StringType
      case "binary" => BinaryType
      case "boolean" => BooleanType
      case "timestamp" => TimestampType
      case "date" => DateType
      case _ => StringType
    }

    //Read Header from Schema File
    val _header = spark.read
      .format("csv")
      .load(_schemaFile)
      .first()
      .mkString(",")

    //Build Schema
    val _schema: StructType = StructType(_header.split(",")
      .map(_colName => StructField(_colName.substring(0, _colName.indexOf(":")), _inferType(_colName), nullable = true)))

    //Schema Names
    val _colNames = _header.split(",")
      .map(colName => colName.substring(0,colName.indexOf(":")).toUpperCase()).toSeq

    val _fileFormat = _configMap("fileFormat")
    val _inferSchemaFlag = _configMap("inferSchemaFlag")
    val _headerFlag = _configMap("headerFlag")
    val _delimiter = _configMap("delimiter")
    val _quoteChar = _configMap("quoteChar")

    try{

    val df_raw = spark.read.format(_fileFormat)
      .schema(_schema)
      .option("inferSchema", _inferSchemaFlag)
      .option("header", _headerFlag)
      .option("sep", _delimiter)
      .option("quote", _quoteChar)
      .load(_srcFile)
      .toDF(_colNames: _*)

      df_raw.write.format("orc").mode("overwrite").saveAsTable(_dbName + "." + _tgtTblName)

    } catch {
      case ex : Exception => System.out.println(ex.printStackTrace())
    } finally {
      spark.stop()
    }

  }
}