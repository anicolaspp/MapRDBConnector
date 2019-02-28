package com.github.anicolaspp.spark.sql

import java.util

import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.v2.reader.{DataReaderFactory, DataSourceReader, SupportsPushDownFilters, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType

class MapRDBDataSourceReader(schema: StructType, tablePath: String)
  extends DataSourceReader
    with SupportsPushDownFilters
    with SupportsPushDownRequiredColumns {

  import collection.JavaConversions._

  private var supportedFilters: List[Filter] = List.empty

  //  private val requiredSchema: StructType = schema

  private var projections: Option[StructType] = None

  override def readSchema(): StructType = projections match {
    case None => schema
    case Some(fieldsToProject) => fieldsToProject
  }

  override def createDataReaderFactories(): util.List[DataReaderFactory[Row]] =
    List(new MapRDBDataReaderFactory(tablePath, supportedFilters, readSchema()))

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {

    println("pushFilters: " + filters.foldLeft("")((s, f) => s + f.toString))

    val (supported, unsupported) = filters.partition {
      case And(_,_) => true
      case Or(_, _) => true
      case EqualTo(_, _) => true
      case GreaterThan(_, _) => true
      case IsNotNull(_) => true

      case _ => false
    }

    supportedFilters = supported.toList

    unsupported
  }

  override def pushedFilters(): Array[Filter] = supportedFilters.toArray

  override def pruneColumns(requiredSchema: StructType): Unit = projections = Some(requiredSchema)
}