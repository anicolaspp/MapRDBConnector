package com.github.anicolaspp.spark.sql

import java.util

import org.apache.spark.sql.Row
import org.apache.spark.sql.sources._
import org.apache.spark.sql.sources.v2.reader.{DataReaderFactory, DataSourceReader, SupportsPushDownFilters, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.types.StructType
import com.mapr.db.spark.MapRDBSpark

class MapRDBDataSourceReader(schema: StructType, tablePath: String)
  extends DataSourceReader
    with SupportsPushDownFilters
    with SupportsPushDownRequiredColumns {

  import collection.JavaConversions._

  private var supportedFilters: List[Filter] = List.empty

  private var projections: Option[StructType] = None

  override def readSchema(): StructType = projections match {
    case None                  => schema
    case Some(fieldsToProject) => fieldsToProject
  }

  override def createDataReaderFactories(): util.List[DataReaderFactory[Row]] = {
    com.mapr.db.MapRDB.getTable(tablePath).getTabletInfos
      .map(descriptor => new MapRDBDataReaderFactory(tablePath, supportedFilters, readSchema(), descriptor)).toList
  }

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    
    val (supported, unsupported) = filters.partition {
      case _: And => true
      case _: Or => true
      case _: IsNull => true
      case _: IsNotNull => true
      case _: In  => true
      case _: StringStartsWith => true
      case _: EqualTo => true
      case _: LessThan => true
      case _: LessThanOrEqual => true
      case _: GreaterThan => true
      case _: GreaterThanOrEqual => true

      case _ => false
    }

    supportedFilters = supported.toList

    unsupported
  }

  override def pushedFilters(): Array[Filter] = supportedFilters.toArray

  override def pruneColumns(requiredSchema: StructType): Unit = projections = Some(requiredSchema)
}
