package com.bigdatalabs.stable.utils
/*
* Author : Anand
* Date : 10-Aug-2024
* Description: Struct Generator
*/

import scala.io._

class preparedStatementGenerator {

  def getStatement(args: String): String = {

    //Fetch Property File Path from Input Parameter
    val _SQLFilePath = args

    var _SQLFile: Source=null
    var _SQLStmt:String=null

    try {

      _SQLFile = Source.fromFile(_SQLFilePath.trim)

      _SQLStmt = _SQLFile.getLines().mkString.trim

      _SQLFile.close()

      if (_SQLStmt == null) {
        println("Undefined SQL - Exiting")
        System.exit(1)
      }

    } catch {
      case ex: Exception =>
        System.out.println(ex.printStackTrace())
        System.exit(2)
    }

    //Return
    _SQLStmt
  }
}