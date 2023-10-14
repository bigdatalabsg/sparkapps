package com.bigdatalabs.stable

import org.apache.spark.sql.SparkSession

object sparkExecuteSQLStatement{
  def main (args: Array[String]): Unit= {

    if (args.length < 2) {
      System.err.println(
        s"""
           |Usage: Spark SQL Runner <DB Name> <SQL>
        """.stripMargin)
      System.exit(1)
    }

    //Read Input
    val _dbName = args(0)
    val _SQL = args(1)

    //println(sqlQuery)
    val whl = "hdfs://localhost:9000/user/hive/warehouse"

    val spark = SparkSession.builder()
      .master("local[*]")//.master("spark://127.0.0.1:7077")
      .appName("sql runner")
      .config("spark.sql.warehouse.location", whl)
      .config("hive.metastore.uris","thrift://localhost:9083")
      .config("spark.dynamicAllocation.enabled", "false")
      .enableHiveSupport()
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    try{

      //Set Current Database
      spark.catalog.setCurrentDatabase(_dbName)
      //Execute SQL
      val df = spark.sql(_SQL)
      //Display
      df.show(false)

      println("Done")

    } catch {
      case ex : Exception => {
        println(ex.printStackTrace())
      }
    } finally {
      spark.stop()
    }

  }
}