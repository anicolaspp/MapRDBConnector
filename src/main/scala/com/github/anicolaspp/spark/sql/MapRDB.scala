package com.github.anicolaspp.spark.sql

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}


object MapRDB {

  implicit class ExtendedSession(sparkSession: SparkSession) {

    def loadFromMapRDB(path: String, schema: StructType): DataFrame = {
      sparkSession
        .read
        .format("com.github.anicolaspp.spark.sql.Reader")
        .schema(schema)
        .load(path)
    }

//    def loadFromMapRDB(path: String, schema: StructType, idxs: String*): DataFrame = {
//      sparkSession
//        .read
//        .format("com.github.anicolaspp.spark.sql.Reader")
//        .option("idx", idxs.mkString(","))
//        .schema(schema)
//        .load(path)
//    }
  }

}