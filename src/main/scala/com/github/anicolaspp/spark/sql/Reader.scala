package com.github.anicolaspp.spark.sql

import java.util

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.sources.v2.{DataSourceOptions, ReadSupportWithSchema}
import org.apache.spark.sql.sources.{EqualTo, Filter, GreaterThan}
import org.apache.spark.sql.sources.v2.reader._
import org.apache.spark.sql.types.StructType


class Reader extends ReadSupportWithSchema {

  override def createReader(schema: StructType, options: DataSourceOptions): DataSourceReader = {

    val tablePath = options.get("path").get()

    new MapRDBDataSourceReader(schema, tablePath)
  }
}






