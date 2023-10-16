**INTRODUCTION**

**Spark Scala Applications on Scala 2.13.12 and Spark 3.3.1**

**1. sparkExecuteSQLStatement.scala - Simple Spark SQL Executor**

_Usage : spark-submit --class com.bigdatalabs.stable.util.sparkExecuteSQLStatement sparkapps-.jar <dbName> <Prepared SQL>_

_examples: <dbName> <Prepared SQL>_

* _"default" "show databases"_
* _"stocks" "select * from t_trades limit 10"_
* _"default" "select * from stocks.t_trades limit 10"_

2. sparkBatchSinkHDFS.scala - Raw CSV File to HDFS in ORC Format
 

3. sparkStreamingMicroBatch.scala - Generic Module for Reading a Kafka Stream and Apply Schema