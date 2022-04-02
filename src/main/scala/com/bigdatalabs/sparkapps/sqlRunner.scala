package com.bigdatalabs.sparkapps

import org.apache.spark.sql.SparkSession
import com.bigdatalabs.sparkutilities.sparkCustomLogger

object sqlRunner{
  def main (args: Array[String]): Unit= {

    println("Usage: Spark SQL Runner <sql query database.table>")

    if (args.length < 1) {
      System.err.println(
        s"""
           |Usage: Spark SQL Runner <sql query database.table>
        """.stripMargin)
      System.exit(1)
    }

    //Read Input
    val sqlQuery = args(0)
    //println(sqlQuery)
    val whl = "hdfs://localhost:9000/user/hive/warehouse"

    val spark = SparkSession.builder()
      .master("local[*]")
      //.master("spark://127.0.0.1:7077")
      .appName("sparksqlrunner")
      .config("spark.sql.warehouse.location", whl)
      .config("spark.shuffle.service.enabled", "false")
      .config("spark.dynamicAllocation.enabled", "false")
      .config("hive.metastore.uris","thrift://localhost:9083")
      .enableHiveSupport()
      .getOrCreate()

    //spark.sparkContext.setLogLevel("INFO")
    //spark.sparkContext.setLogLevel("ERROR")
    spark.sparkContext.setLogLevel("WARN")
    sparkCustomLogger.setStreamingLogLevels()

    //List Databases
    spark.catalog.listDatabases().show(10,150)

    //Fetch Records
    val df = spark.sql(sqlQuery)

    //Apply Transformations, Aggregations
    //df.groupBy("symbol").avg("open","high","low" ,"close","volume","adj_close").show()
    df.show()

    println("Done")

    spark.stop()

  }
}