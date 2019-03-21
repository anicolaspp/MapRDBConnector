package com.github.anicolaspp

import com.github.anicolaspp.spark.sql.reading.JoinType
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}


// ONLY for testing
object App {

  import com.github.anicolaspp.spark.MapRDB._

  def main(args: Array[String]): Unit = {

    val config = new org.apache.spark.SparkConf().setAppName("testing streaming")

    val sparkSession = SparkSession
      .builder()
      .config(config)
      .getOrCreate()

    sparkSession.conf.set("spark.sql.streaming.checkpointLocation", "/Users/nperez/check")
    sparkSession.conf.set("spark.sql.streaming.schemaInference", value = true)

    sparkSession.sparkContext.setLogLevel("WARN")

    println("HERE")


      val rdd = sparkSession.sparkContext.parallelize(1 to 1000000).map(n => Row(n.toString))
      .union(sparkSession.sparkContext.parallelize(List(Row("67-34859-102-69068124-28-931853-80564775-5573-4-552141-1162559-508125-8114-59-498052941-123-7085126119-8020-460105-36-56-126-3067-338569-14116-120102731011680117-47-8778-48-1121074158-47-111-4868424292971077-105120-1711-404-6-5696-11445052-6664-54-11739"))))


    val df = sparkSession.createDataFrame(rdd, new StructType().add("value", StringType))

    val inner_join = df.joinWithMapRDBTable(
      "/user/mapr/tables/from_parquet",
      new StructType().add("_id", StringType).add("payload", StringType),
      "value",
      "payload")(sparkSession)


    println("INNER JOIN: ")
    inner_join.printSchema()
    println(inner_join.count)

    val left_join = df.joinWithMapRDBTable("/user/mapr/tables/from_parquet",
      new StructType().add("_id", StringType).add("payload", StringType),
      "value",
      "payload",
      JoinType.left)(sparkSession)

    println("LEFT JOIN: ")
    left_join.printSchema()
    println(left_join.count)


    val left_outer = df.joinWithMapRDBTable("/user/mapr/tables/from_parquet",
      new StructType().add("_id", StringType).add("payload", StringType),
      "value",
      "payload",
      JoinType.left_outer)(sparkSession)

    println("LEFT OUTER JOIN: ")
    left_outer.printSchema()
    println(left_outer.count)

    val outer = df.joinWithMapRDBTable("/user/mapr/tables/from_parquet",
      new StructType().add("_id", StringType).add("payload", StringType),
      "value",
      "payload",
      JoinType.outer)(sparkSession)

    println("OUTER JOIN: ")
    outer.printSchema()
    println(outer.count)
  }
}