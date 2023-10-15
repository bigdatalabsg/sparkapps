package com.bigdatalabs.stable.streaming

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.io.{FileNotFoundException, IOException}
import scala.io.{BufferedSource, Source}

object sparkKafkaStreamingMicroBatch extends Serializable {

  def main(args: Array[String]): Unit = {

    //Variables

    var _configFile : BufferedSource = null

    var _brokers: String = null
    var _subsTopic: String = null
    var _msgSchemaFile: String = null
    var _groupId: String = null
    var _microbatchSecs: Int = 0
    var _streamFormat: String= null
    var _lineSplitterChar: String = null
    var _quoteChar: String = null
    var _delimiterChar: String = null

    //Spark Context
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("kafka stream handler")
      .getOrCreate()

    //Fetch Property File Path from Input Parameter
    val _prop_file_path = args(0)

    //Check for Propoerties File
    try {
      print("=======================================================================\n")
      println("RESOURCE FILE:" + _prop_file_path)
      print("=======================================================================\n")

      _configFile = Source.fromFile(_prop_file_path)

    } catch {
      case ex: FileNotFoundException => println(ex.printStackTrace())
      case ex: IOException => println(ex.printStackTrace())
    } finally {

    }

    //Read Application Config
    val _configMap = _configFile.getLines().filter(line => line.contains("=")).map { line =>
      val tkns = line.split("=")
      if (tkns.size == 1) {
        tkns(0) -> ""
      } else {
        tkns(0) -> tkns(1)
      }
    }.toMap

    _brokers=_configMap("brokers")
    _subsTopic=_configMap("subsTopic")
    _msgSchemaFile=_configMap("msgSchemaFile")
    _streamFormat = _configMap("fileFormat")
    _groupId=_configMap("groupId")
    _microbatchSecs=_configMap("microbatchSecs").toInt
    _lineSplitterChar=_configMap("lineSplitterChar")
    _delimiterChar=_configMap("delimiterChar")
    _quoteChar=_configMap("quoteChar")

    //WHY?
    val topicsSet = _subsTopic.split(",").toSet

    //Set Logging Level
    spark.sparkContext.setLogLevel("WARN")

    //Set Streaming Context
    val ssc = new StreamingContext(spark.sparkContext, Seconds(_microbatchSecs))

    //Kafka Params
    val kafkaParams = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> _brokers,
      ConsumerConfig.GROUP_ID_CONFIG -> _groupId,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer]
    )

    //Read from Kafka
    val _readStream = KafkaUtils.createDirectStream[String, String](ssc,LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams))

    def _inferType(field: String) = field.split(":")(1) match {
      case "byte" => ByteType
      case "short" => ShortType
      case "integer" => IntegerType
      case "long" => LongType
      case "float" => FloatType
      case "double" => DoubleType
      //case "decimal" => java.math.BigDecimal
      case "string" => StringType
      case "binary" => BinaryType
      case "boolean" => BooleanType
      case "timestamp" => TimestampType
      case "date" => DateType
      case _ => StringType
    }

    //Read Header from Schema File
    val _header = spark.read
      .format("csv")
      .load(_msgSchemaFile)
      .first()
      .mkString(",")

    //Build Schmema Dyanmically from Schema File
    val _msgSchema: StructType = StructType(_header.split(",")
      .map(_colName => StructField(_colName.substring(0, _colName.indexOf(":")), _inferType(_colName), nullable = true)))

    try{
      //Read the Value Pair, Ignore Key
      val _processStream = _readStream.map(line => line.value)

      //Process Record
      _processStream.foreachRDD(
        _rddRecord => {
          import spark.implicits._
          val _rawDF = _rddRecord.toDF("results")
          val df_Trade = _rawDF.select(from_json($"results",_msgSchema) as "data").select("data.*")
          //val df_Trade = _rawDF.select(from_json($"results", _msgSchema) as "data").select("data.*")
          //df_Trade.cache()
          df_Trade.printSchema()
          //val df_agg = df_Trade.groupBy("symbol").agg(avg($"volume").alias("avg_vol"))
          print(df_Trade.show(false))
        })

      // Start the computation
      ssc.start()
      ssc.awaitTermination()

    } catch {
      case ex: Exception => ex.printStackTrace()
    } finally {
      ssc.stop(stopSparkContext = true, stopGracefully = true)
    }
  }
}