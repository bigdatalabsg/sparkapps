**INTRODUCTION**

**Spark Scala Applications on Scala 2.13.12 and Spark 3.3.1**

**1. sparkExecuteSQLStatement.scala - Simple Spark SQL Executor**

_Usage : spark-submit --class com.bigdatalabs.stable.util.sparkExecuteSQLStatement sparkapps-.jar <dbName> <Prepared SQL>_

_examples: <dbName> <Prepared SQL>_

* _"default" "show databases"_
* _"stocks" "select * from t_trades limit 10"_
* _"default" "select * from stocks.t_trades limit 10"_

**2. sparkBatchSinkHDFS.scala - Batch Ingest CSV File to Hive in ORC Format**
 


**3. sparkStreamingMicroBatch.scala - Spark Streaming with Dynamic Schema**  



**4. sparkStreamingAvroProducer.scala - JSON to AVRO Producer with Dynamic Schema**



**5. sparkStreamingAvroConsumer.scala - Kafka Consumer for Avro with Dynamic Schema**



**6. sparkBatchIcebergSink.scala - Batch Ingestion for Iceberg Non Partitioned Tables**



**7. sparkBatchWithPartitionIcebergSink - Batch Ingestion for Iceberg Partitioned Tables**



**8. sparkStreamIcebergSink.scala - Streaming Ingestion for Iceberg Non Partitioned Tables**



**9. sparkStreamPartitionIcebergSink.scala - Streaming Ingestion for Iceberg Partitioned Tables**
