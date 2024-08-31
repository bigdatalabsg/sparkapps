package com.bigdatalabs.utils

/*
* Author : Anand
* Date : 17-Oct-2023
* Description: Struct Generator
*/

import org.apache.spark.sql.types._

import scala.io.Source

class schemaGenerator {

    def getSchema(args: String): StructType = {

        val _schemaFilePath: String = args
        var _schemaFile: Source = null

        var _schemaHeader: String = null

        var _schema: StructType = null

        _schemaFile = Source.fromFile(_schemaFilePath.trim)

        _schemaHeader = _schemaFile.getLines().mkString.trim

        //Infer Schema, Split the String [<Column Name> : <Data Type> ] at ":"
        def _inferType(field: String) = field.split(":")(1) match {
            case "byte" => ByteType
            case "short" => ShortType
            case "integer" => IntegerType
            case "long" => LongType
            case "float" => FloatType
            case "double" => DoubleType
            //case "decimal" => org.apache.spark.sql.types.DecimalType
            case "string" => StringType
            case "binary" => BinaryType
            case "boolean" => BooleanType
            case "timestamp" => TimestampType
            case "date" => DateType
            case _ => StringType
        }

        try {
            //Build Schema
            _schema = StructType(_schemaHeader.split(",")
              .map(_colName => StructField(_colName.substring(0, _colName.indexOf(":")), _inferType(_colName), nullable = true)))

        } catch {
            case ex: Exception =>
                System.out.println(ex.printStackTrace())
                System.exit(2)
        } finally {
            _schemaFile.close()
        }

        //Return Struct Type Object
        _schema

    }

}
