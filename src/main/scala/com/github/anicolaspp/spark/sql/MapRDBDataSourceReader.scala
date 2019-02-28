package com.github.anicolaspp.spark.sql

import java.util

import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.v2.reader.{DataReaderFactory, DataSourceReader, SupportsPushDownFilters, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.sources.{EqualTo, Filter, GreaterThan}
import org.apache.spark.sql.types.StructType

class MapRDBDataSourceReader(schema: StructType, tablePath: String)
  extends DataSourceReader
    with SupportsPushDownFilters
    with SupportsPushDownRequiredColumns {

  private var supportedFilters: List[Filter] = List.empty

  private val requiredSchema: StructType = schema

  private var projections: Option[StructType] = None

  override def readSchema(): StructType = projections match {
    case None => requiredSchema
    case Some(p) => p
  }

  override def createDataReaderFactories(): util.List[DataReaderFactory[Row]] = {
    val li = new util.ArrayList[DataReaderFactory[Row]]

    println("MapRDBDataSourceReader.createDataReaderFactories: READ SCHEMA:" + readSchema())

    li.add(new MapRDBDataReaderFactory(tablePath, supportedFilters, readSchema()))

    li
  }

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    val (supported, unsupported) = filters.partition {
      case EqualTo(_, _) => true
      case GreaterThan(_, _) => true

      case _ => false
    }

    supportedFilters = supported.toList

    unsupported
  }

  override def pushedFilters(): Array[Filter] = supportedFilters.toArray

  override def pruneColumns(requiredSchema: StructType): Unit = {
    println("MapRDBDataSourceReader.pruneColumns: pruneColumns SCHEMA:" + requiredSchema)

    this.projections = Some(requiredSchema)
  }
}
