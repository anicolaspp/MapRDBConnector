package com.github.anicolaspp.spark.sql

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}


object MapRDB {

  implicit class ExtendedSession(sparkSession: SparkSession) {

    def loadFromMapRDB(path: String, schema: StructType): DataFrame = {
      sparkSession
        .read
        .format("com.github.anicolaspp.spark.sql.reading.Reader")
        .schema(schema)
        .load(path)
    }

  }

  implicit class ExtendedDataFrame(df: DataFrame) {
    def saveToMapRDB(path: String, withTransaction: Boolean = false): Unit = {

      if (withTransaction) {
        df.write
          .format("com.github.anicolaspp.spark.sql.writing.Writer")
          .save(path)

      } else {

        com.mapr.db.spark.sql
          .MapRDBDataFrameFunctions(df)
          .saveToMapRDB(path)
      }


    }
  }

}