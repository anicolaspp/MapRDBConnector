package com.github.anicolaspp.spark.sql.reading

import org.apache.spark.internal.Logging
import org.apache.spark.sql.sources.v2.reader._
import org.apache.spark.sql.sources.v2.{DataSourceOptions, ReadSupportWithSchema}
import org.apache.spark.sql.types.StructType

class Reader extends ReadSupportWithSchema with Logging {

  override def createReader(schema: StructType, options: DataSourceOptions): DataSourceReader = {

    val tablePath = options.get("path").get()

    log.debug(s"TABLE PATH BEING USED: $tablePath")

    val hintedIndexes = options.get("idx").orElse("").trim.split(",").toList

    new MapRDBDataSourceReader(schema, tablePath, hintedIndexes)
  }
}