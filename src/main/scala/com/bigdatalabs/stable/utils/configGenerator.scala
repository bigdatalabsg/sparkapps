package com.bigdatalabs.stable.utils

/*
* Author : Anand
* Date : 09-Aug-2024
* Description: Struct Generator
*/

import scala.io._

class configGenerator {

  def getParams(args: String): Map[String, String] = {

    val _path: String = args

    var _configFile: BufferedSource = null
    var _configMap: Map[String, String] = null
    var _configTokens: Array[String] = null

    _configFile = Source.fromFile(_path)

    //Read Application Config
    _configMap = _configFile.getLines().filter(line => line.contains("::")).map { line =>
      _configTokens = line.split("::")
      if (_configTokens.length == 1) {
        _configTokens(0) -> ""
      } else {
        _configTokens(0) -> _configTokens(1)
      }
    }.toMap

    _configFile.close()

    //return config Maps
    _configMap

  }
}