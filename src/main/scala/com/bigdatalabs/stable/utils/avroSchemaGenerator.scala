package com.bigdatalabs.stable.utils

import scala.io.Source

class avroSchemaGenerator {

  def returnAvroSchema(args: String): String = {

    //Fetch Property File Path from Input Parameter
    val _avroSchemaFilePath = args

    var _schemaFile: Source = null

    var _avroSchema: String = null

    try {

      _schemaFile = Source.fromFile(_avroSchemaFilePath.trim)
      _avroSchema = _schemaFile.getLines().mkString.trim
      _schemaFile.close()

      if (_avroSchema == null) {
        println("Undefined SQL - Exiting")
        System.exit(1)
      }

    } catch {
      case ex: Exception =>
        System.out.println(ex.printStackTrace())
        System.exit(2)
    }

    //Return
    _avroSchema

  }

}