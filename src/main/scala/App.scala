package com.github.anicolaspp

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}


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

    //    sparkSession.sparkContext.setLogLevel("DEBUG")

    println("HERE")

    val schema = StructType(Seq(StructField("_id", StringType), StructField("payload", StringType))) //StructField("first_name", StringType), StructField("uid", StringType)))
//
//        val data = sparkSession
//          .loadFromMapRDB("/user/mapr/tables/from_parquet", schema)
//          .filter("payload = '67-34859-102-69068124-28-931853-80564775-5573-4-552141-1162559-508125-8114-59-498052941-123-7085126119-8020-460105-36-56-126-3067-338569-14116-120102731011680117-47-8778-48-1121074158-47-111-4868424292971077-105120-1711-404-6-5696-11445052-6664-54-11739'")
//          .select("_id", "payload")
//
//        println(s"MY SCHEMA: ${data.schema}")
//
//        println(s"COUNT: ${data.count()}")


    val rdd = sparkSession.sparkContext.parallelize(1 to 1000000).map(n => Row(n.toString))
      .union(sparkSession.sparkContext.parallelize(List(Row("67-34859-102-69068124-28-931853-80564775-5573-4-552141-1162559-508125-8114-59-498052941-123-7085126119-8020-460105-36-56-126-3067-338569-14116-120102731011680117-47-8778-48-1121074158-47-111-4868424292971077-105120-1711-404-6-5696-11445052-6664-54-11739"))))
      .repartition(200)

    val df = sparkSession.createDataFrame(rdd, new StructType().add("value", StringType))

    val joint = df.joinWithMapRDBTable("/user/mapr/tables/from_parquet", schema, "value", "payload")(sparkSession)

    joint.printSchema()
    joint.show(10)
  }
}