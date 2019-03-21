package com.github.anicolaspp.spark


import com.mapr.db.spark.utils.MapRSpark
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
    def writeToMapRDB(path: String, withTransaction: Boolean = false): Unit =
      if (withTransaction) {
        df.write
          .format("com.github.anicolaspp.spark.sql.writing.Writer")
          .save(path)

      } else {
        MapRSpark.save(df, path, "_id", false, false)
      }
  }

}
