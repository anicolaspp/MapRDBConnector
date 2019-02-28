package com.github.anicolaspp

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}


// ONLY for testing
object App {

  import com.github.anicolaspp.spark.sql.MapRDB._

  def main(args: Array[String]): Unit = {

    val config = new org.apache.spark.SparkConf().setAppName("testing streaming")

    val sparkSession = SparkSession
      .builder()
      .config(config)
      .getOrCreate()

    sparkSession.conf.set("spark.sql.streaming.checkpointLocation", "/Users/nperez/check")
    sparkSession.conf.set("spark.sql.streaming.schemaInference", value = true)

    println("HERE")

    val data = sparkSession
      .loadFromMapRDB("/user/mapr/tables/data", StructType(Seq(StructField("_id", StringType), StructField("uid", StringType))))
      .filter("uid = '101'")
      .select("_id")


    data.take(3).foreach(println)
  }
}