package com.bigdatalabs.utils

import org.apache.spark.sql.SparkSession

class runIcebergMaintenance {

  def rewriteDataFiles(_catalogName: String, _dbName: String, _tgtTblName: String, _filterSet: String): Boolean = {

    val spark = SparkSession.builder
      .master("local[*]") //"spark://localhost:7077"
      .appName("iceberg-maintenance-rewrite-manifest")
      .getOrCreate()

    val _preparedStatement = "CALL" + _catalogName + ".system.rewrite_data_files(" + _dbName + "." + _tgtTblName + ")"

    try {
      spark.sql(_preparedStatement)
    } catch {
      case ex: Exception =>
        println(ex.printStackTrace())
    } finally {
      spark.stop()
    }

    //Return
    true

  }

  def rewriteManifest(_catalogName: String, _dbName: String, _tgtTblName: String, _filterSet: String): Boolean = {

    val spark = SparkSession.builder
      .master("local[*]") //"spark://localhost:7077"
      .appName("iceberg-maintenance-rewrite-manifest")
      .getOrCreate()

    val _preparedStatement = "CALL" + _catalogName + ".system.rewrite_datafiles(" + _dbName + "." + _tgtTblName + ")"

    try {
      spark.sql(_preparedStatement)
    } catch {
      case ex: Exception =>
        println(ex.printStackTrace())
    } finally {
      spark.stop()
    }

    //Return
    true

  }

}