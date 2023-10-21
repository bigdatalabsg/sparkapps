/*
* Author : Anand
* Date : 17-Oct-2023
* Description: Struct Generator
*/

package com.bigdatalabs.stable.utils

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

import java.io.{FileNotFoundException, IOException}

class generateSchema {

    def getStruct(args: String): StructType = {

        var _schema: StructType = null

        val spark = SparkSession.builder
          .master("local[*]") //"spark://localhost:7077"
          .appName("data frame struct generator utility")
          .getOrCreate()

        //Fetch Property File Path from Input Parameter
        val _schemaFile = args

        //Check for Propoerties File
        try {
            print("=======================================================================\n")
            println("SCHEMA FILE:" + _schemaFile)
            print("=======================================================================\n")
        } catch {
            case ex: FileNotFoundException =>
                println(ex.printStackTrace())
                System.exit(1)

            case ex: IOException =>
                println(ex.printStackTrace())
                System.exit(2)
        }

        //Infer Schema, Split the String [<Column Name> : <Data Type> ] at ":"
        def _inferType(field: String) = field.split(":")(1) match {
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

        try {
            //Read Header from Schema File
            val _header = spark.read
              .format("csv")
              .load(_schemaFile)
              .first()
              .mkString(",")

            //Build Schema
            _schema = StructType(_header.split(",")
              .map(_colName => StructField(_colName.substring(0, _colName.indexOf(":")), _inferType(_colName), nullable = true)))

            //Schema Names
//            val _colNames = _header.split(",")
//              .map(colName => colName.substring(0, colName.indexOf(":")).toUpperCase()).toSeq

        } catch {
            case ex: Exception =>
                System.out.println(ex.printStackTrace())
                System.exit(2)
        }

        //Return Struct Type Object
        return _schema

    }
}