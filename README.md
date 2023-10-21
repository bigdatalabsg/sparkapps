## **Spark Scala Applications on Scala 2.13.12 and Spark 3.3.1**

### **1. sparkExecuteSQL.scala - Simple Spark SQL Executor**

#### _Usage : spark-submit --class com.bigdatalabs.stable.util.sparkExecuteSQLStatement sparkapps-<version>.jar [dbName] [Prepared SQL]_

##### _examples: [dbName] [Prepared SQL]_

##### * _"default" "show databases"_
##### * _"stocks" "select * from t_trades limit 10"_
##### * _"default" "select * from stocks.t_trades limit 10"_

### **2. sparkBatchSinkHDFS.scala - Batch Ingest CSV File to Hive in ORC Format**
 

#### _Usage : spark-submit --class com.bigdatalabs.stable.batch.sparkBatchSinkHDFS sparkapps-<version>.jar [config file path/config file name]_


### **3. sparkBatchIcebergSink.scala - Batch Ingestion for Iceberg Non Partitioned Tables**


#### _Usage : spark-submit --class com.bigdatalabs.stable.streaming.sparkStreamIcebergSink sparkapps-<version>.jar [config file path/config file name]_


### **4. sparkBatchWithPartitionIcebergSink - Batch Ingestion for Iceberg Partitioned Tables**


#### _Usage : spark-submit --class com.bigdatalabs.stable.streaming.sparkStreamPartitionIcebergSink sparkapps-<version>.jar [config file path/config file name]_


### **5. sparkStreamingMicroBatch.scala - Spark Streaming with Dynamic Schema**

#### _Usage : spark-submit --class com.bigdatalabs.stable.streaming.sparkStreamingMicroBatch sparkapps-<version>.jar [config file path/config file name]_


### **6. sparkStreamingAvroProducer.scala - Spark Kafka Producer with Dynamic Schema**


#### _Usage : spark-submit --class com.bigdatalabs.stable.streaming.sparkStreamingAvroProducer sparkapps-<version>.jar [config file path/config file name]_


### **7. sparkStreamingAvroConsumer.scala - Spark Kafka Consumer for Avro with Dynamic Schema**


#### _Usage : spark-submit --class com.bigdatalabs.stable.streaming.sparkStreamingAvroConsumer sparkapps-<version>.jar [config file path/config file name]_


### **8. sparkStreamIcebergSink.scala - Streaming Ingestion for Iceberg Non Partitioned Tables**


#### _Usage : spark-submit --class com.bigdatalabs.stable.streaming.sparkStreamIcebergSink sparkapps-<version>.jar [config file path/config file name]_


### **9. sparkStreamPartitionIcebergSink.scala - Streaming Ingestion for Iceberg Partitioned Tables**


#### _Usage : spark-submit --class com.bigdatalabs.stable.streaming.sparkStreamPartitionIcebergSink sparkapps-<version>.jar [config file path/config file name]_