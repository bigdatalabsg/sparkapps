package com.bigdatalabs.utils

/*
* Author : Anand
* Date : 10-Aug-2024
* Description: Struct Generator
*/

import scala.io._

class preparedStatementGenerator {

  def getStatement(args: String): String = {

    //Fetch Property File Path from Input Parameter
    val _preparedStatementFilePath:String = args
    var _preparedStatementFile: Source=null
    var _preparedStatement:String=null

    try {

      _preparedStatementFile = Source.fromFile(_preparedStatementFilePath.trim)
      _preparedStatement = _preparedStatementFile.getLines().mkString.trim

      if (_preparedStatement == null) {
        println("Undefined Prepared Statement - Exiting")
        System.exit(1)
      }
    } catch {
      case ex: Exception =>
        System.out.println(ex.printStackTrace())
        System.exit(2)
    } finally {
      _preparedStatementFile.close()
    }
    //Return
    _preparedStatement
  }
}