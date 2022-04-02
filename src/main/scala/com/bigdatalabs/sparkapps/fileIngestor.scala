package com.bigdatalabs.sparkapps

import com.bigdatalabs.sparkutilities
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object fileIngestor {

  def main(args: Array[String]): Unit = {

    sparkutilities.sparkCustomLogger

    if (args.length < 4) {
      System.err.println(
        s"""
           |Usage: Spark File Ingestor </location/Source file Name> <SchemaFile> <dbName> <target table>
        """.stripMargin)
      System.exit(1)
    }

    val _srcFileName=args(0) //source file name
    val _schemaFile=args(1)  //schema file
    val _dbName = args(2)    //dbname
    val _tgtTableNameRAW = args(3) + "_raw" // raw
    val _tgtTableNameORC = args(3) + "_orc" //convert to orc

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

    val _whloc = "hdfs://localhost:9000/user/hive/warehouse" //warehouse location
    val _thrftSrvr = "thrift://localhost:9083"  //hive thrift uri & port

    //Session
    val spark = SparkSession.builder
      //.master("spark://localhost:7077")
      .master("local[*]")
      .appName("spark file Ingestor")
      .config("spark.sql.warehouse.location", _whloc)
      .config("hive.metastore.uris",_thrftSrvr)
      .enableHiveSupport()
      .getOrCreate()

    //Read Header from Schema File
    val _header = spark.read
      .format("CSV")
      .load(_schemaFile)
      .first()
      .mkString(",")

    //Build Schmema
    val _schema = StructType(_header.split(",")
      .map(_colName => StructField(_colName,_inferType(_colName),true)))

    //_schema.printTreeString()

    //Schema Names
    val _colNames = _header.split(",")
      .map(colName => colName.substring(0,colName.indexOf(":")).toUpperCase()).toSeq

    val file_type = "CSV"
    val infer_schema = "false"
    val first_row_is_header = "false"
    val delimiter = ","

    val df_raw = spark.read.format(file_type)
      .schema(_schema)
      .option("inferSchema", infer_schema)
      .option("header", first_row_is_header)
      .option("sep", delimiter)
      .load(_srcFileName)
      .toDF(_colNames: _*)

    //df_raw.printSchema()

    df_raw.write.mode("overwrite")
      .saveAsTable(_dbName + "." + _tgtTableNameRAW)

    spark.sql("DROP TABLE IF EXISTS " + _tgtTableNameORC)

    spark.sql("CREATE TABLE IF NOT EXISTS "
      + _dbName + "." +_tgtTableNameORC
      + " STORED AS ORC AS "
      + "SELECT * FROM " + _dbName + "." + _tgtTableNameRAW)

    val df_orc = spark.sql("SELECT * FROM " + _dbName + "." + _tgtTableNameORC)

    val new_df = df_raw.union(df_orc)

    new_df.show(10, false)

    spark.stop()

    }
}