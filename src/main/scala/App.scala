package com.github.anicolaspp

import org.apache.spark.sql.SparkSession
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

    sparkSession.sparkContext.setLogLevel("DEBUG")

    println("HERE")

    val schema = StructType(Seq(StructField("_id", StringType), StructField("_2", StringType)))//StructField("first_name", StringType), StructField("uid", StringType)))

    sparkSession
      .loadFromMapRDB("/user/mapr/tables/from_parquet", schema, "idx_2")
      .filter("_2 = 'n2078258460719121947'")
//      .filter("uid = '101'")
//      .select("_id", "first_name")
      .show()
    
//    sparkSession
//      .loadFromMapRDB("/user/mapr/tables/data", schema)
//      .filter("uid = '101' and first_name = 'tom'")
//      .show()
//
//    sparkSession
//      .loadFromMapRDB("/user/mapr/tables/data", schema)
//      .filter("uid = '101'")
//      .filter("first_name = 'tom'")
//      .show()
//
//    sparkSession
//      .loadFromMapRDB("/user/mapr/tables/data", schema)
//      .filter("(uid <= '101' or first_name = 'john') and _id = '1'")
//      .show()
//
//    sparkSession
//      .loadFromMapRDB("/user/mapr/tables/data", schema)
//      .filter("uid >= '101' or _id = '1'")
//      .show()

  }
}