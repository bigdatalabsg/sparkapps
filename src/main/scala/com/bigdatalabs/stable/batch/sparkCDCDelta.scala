package com.bigdatalabs.stable.batch

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/*
* Author : Anand
* Date : 15-May-2024
* Description: CDC Delta processing for iceberg
*/

object sparkCDCDelta {
    def main(args: Array[String]): Unit = {

        val whl = "hdfs://localhost:9000/user/hive/warehouse"

        val spark = SparkSession.builder()
          .master("local[*]")
          .appName("sparkApplication")
          .config("spark.sql.warehouse.location", whl)
          .config("spark.shuffle.service.enabled", "false")
          .config("spark.dynamicAllocation.enabled", "false")
          .config("hive.metastore.uris", "thrift://localhost:9083")
          .enableHiveSupport()
          .getOrCreate()

        spark.sparkContext.setLogLevel("WARN")

        //List Databases
        //spark.catalog.listDatabases().show(false)
        import spark.implicits._

        val df_stg = spark.sql("select * from dwh_bronze.t_src_bronze")

        df_stg.cache()
        df_stg.show()

        val df_tgt = spark.sql("select * from dwh_silver.t_src_silver")

        df_tgt.cache()
        df_tgt.show()

        println("1. Perform Full Join between Source and Target")

        val df_joined_data =df_tgt.join( df_stg, df_tgt("sk_id") === df_stg("src_id"), "full" )
        df_joined_data.cache()
        df_joined_data.show(false)

        println("2. Classify with actions")

        val df_enriched = df_joined_data.withColumn("action", when (df_joined_data("attr")=!=df_joined_data("src_attr"),"UPSERT")
            .when(df_joined_data("src_attr").isNull and df_joined_data("is_current"),"DELETE")
            .when(df_joined_data("sk_id").isNull,"INSERT")
            .otherwise("NOACTION"))

        df_enriched.cache()
        df_enriched.show(false)

        val column_names = Seq("sk_id", "attr", "mod_dt", "data_src_cd", "is_current", "eff_start_dt", "eff_end_dt")

        println("3. Expire Record for Changed Records")
        val df_close_recs = df_enriched.filter($"action" === "UPSERT")
          .withColumn("eff_end_dt", df_enriched("src_mod_dt"))
          .withColumn("is_current", lit("false".toBoolean))
          .select(column_names.map(c => col(c)): _*)

        df_close_recs.cache()
        df_close_recs.show(false)

        println("4. Prepare Upsert for Changed Records with High End Date")

        val df_upsert=df_enriched.filter($"action"==="UPSERT")
          .withColumn("attr", $"src_attr")
          .withColumn("mod_dt", $"src_mod_dt")
          .withColumn("is_current",lit(true))
          .withColumn("eff_start_dt", $"src_mod_dt")
          .select(column_names.map(c=>col(c)):_*)

        df_upsert.cache()
        df_upsert.show(false)

        println("5. Prepare for Insering New records with High End Date")

        val df_insert = df_enriched.filter($"action" === "INSERT")
          .withColumn("sk_id", $"src_id")
          .withColumn("attr", $"src_attr")
          .withColumn("mod_dt", $"src_mod_dt")
          .withColumn("data_src_cd", lit("bronze_data"))
          .withColumn("is_current", lit(true))
          .withColumn("eff_start_dt", $"src_mod_dt")
          .withColumn("eff_end_dt", to_date(lit("9999-12-31")))
          .select(column_names.map(c => col(c)): _*)

        df_insert.cache()
        df_insert.show (false)

        println("6. Prepare for No Change Records")
        val df_noaction = df_enriched.filter($"action" === "NOACTION")
          .select(column_names.map(c => col(c)): _*)

        df_noaction.cache()
        df_noaction.show (false)

        println("7. Merge CLOSED, UPSERT, INSERT , NOACTION Records")

        val df_merged_final = df_noaction.union(df_insert).union(df_close_recs).union(df_upsert).orderBy("sk_id")

        df_merged_final.show(false)

    }
}