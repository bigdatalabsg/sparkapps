**INTRODUCTION**

**Spark Scala Applications on Scala 2.13.12 and Spark 3.3.1**

1. sparkExecuteSQLStatement.scala - Simple Spark SQL Executor

Usage : spark-submit --class com.bigdatalabs.stable.util.sparkExecuteSQLStatement sparkapps-.jar <dbName> <Prepared SQL>

example: <dbName> <Prepared SQL>

i) "default" "show databases"
ii) "stocks" "select * from t_trades limit 10"
iii) "default" "select * from stocks.t_trades limit 10"

2. sparkBatchSinkHDFS.scala - Raw CSV File to HDFS in ORC Format
3. sparkStreamingMicroBatch.scala - Generic Module for Reading a Kafka Stream and Apply Schema