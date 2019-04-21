package com.github.anicolaspp.spark.sql.reading

import org.apache.spark.internal.Logging
import org.apache.spark.sql.sources.v2.reader._
import org.apache.spark.sql.sources.v2.{DataSourceOptions, ReadSupportWithSchema}
import org.apache.spark.sql.types.StructType

import scala.util.Try

class Reader extends ReadSupportWithSchema with Logging {

  override def createReader(schema: StructType, options: DataSourceOptions): DataSourceReader = {

    val tablePath = options.get("path").get()

    log.debug(s"TABLE PATH BEING USED: $tablePath")

    val hintedIndexes = options.get("idx").orElse("").trim.split(",").toList

    val readersPerTablet = getNumberOfReaders(options)

    if (readersPerTablet > 1) {
      new MapRDBDataSourceReaderMultiTabletReader(schema, tablePath, hintedIndexes, readersPerTablet)
    } else {
      new MapRDBDataSourceReader(schema, tablePath, hintedIndexes)
    }
  }

  private def getNumberOfReaders(options: DataSourceOptions): Int = Try {
    options.get("readers").orElse("1").toInt
  }.getOrElse(1)
}