package com.github.anicolaspp.spark.sql.reading

import org.apache.spark.internal.Logging
import org.apache.spark.sql.sources.v2.reader._
import org.apache.spark.sql.sources.v2.{DataSourceOptions, ReadSupportWithSchema}
import org.apache.spark.sql.types.StructType

import scala.util.Try

class Reader extends ReadSupportWithSchema with Logging {

  override def createReader(schema: StructType, options: DataSourceOptions): DataSourceReader = {

    val tablePath = options.get("path").get()

    log.info(s"TABLE PATH BEING USED: $tablePath")

    val hintedIndexes = options.get("idx").orElse("").trim.split(",").toList

    val readersPerTablet = getNumberOfReaders(options)

    new MapRDBDataSourceMultiReader(schema, tablePath, hintedIndexes, readersPerTablet)
  }

  private def getNumberOfReaders(options: DataSourceOptions): Int = Try {
    val numberOfReaders = options.get("readers").orElse("1").toInt

    if (numberOfReaders < 1) 1 else numberOfReaders

  }.getOrElse(1)
}