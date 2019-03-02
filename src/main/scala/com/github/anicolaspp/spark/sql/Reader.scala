package com.github.anicolaspp.spark.sql

import java.util.function.Supplier

import org.apache.spark.sql.sources.v2.reader._
import org.apache.spark.sql.sources.v2.{DataSourceOptions, ReadSupportWithSchema}
import org.apache.spark.sql.types.StructType

class Reader extends ReadSupportWithSchema {

  import collection.JavaConversions._
  import collection.JavaConverters._

  override def createReader(schema: StructType, options: DataSourceOptions): DataSourceReader = {

    val tablePath = options.get("path").get()

    val hintedIndexes = options.get("idx").orElse("").trim.split(",").toList

    new MapRDBDataSourceReader(schema, tablePath, hintedIndexes)
  }
}