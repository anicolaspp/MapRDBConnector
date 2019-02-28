package com.github.anicolaspp.spark.sql

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

object MapRDB {


  implicit class ExtendedSession(sparkSession: SparkSession) {

    def loadFromMapRDB(path: String, schema: StructType): DataFrame = {


      // maybe find table partition information here and send it down to the data source

      sparkSession
        .read
        .format("com.github.anicolaspp.spark.sql.Reader")
        .schema(schema)
        .load(path)

    }
  }

}
