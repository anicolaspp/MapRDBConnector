package com.github.anicolaspp.spark.sql.reading

import java.util

import com.github.anicolaspp.spark.sql.MapRDBTabletInfo
import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources._
import org.apache.spark.sql.sources.v2.reader.{DataReaderFactory, DataSourceReader, SupportsPushDownFilters, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.types.StructType

class MapRDBDataSourceReader(schema: StructType, tablePath: String, hintedIndexes: List[String])
  extends DataSourceReader
    with Logging
    with SupportsPushDownFilters
    with SupportsPushDownRequiredColumns {

  import collection.JavaConversions._

  private var supportedFilters: List[Filter] = List.empty

  private var projections: Option[StructType] = None

  override def readSchema(): StructType = projections match {
    case None => schema
    case Some(fieldsToProject) => fieldsToProject
  }

  override def createDataReaderFactories(): util.List[DataReaderFactory[Row]] = {
    com.mapr.db.MapRDB
      .getTable(tablePath)
      .getTabletInfos
      .zipWithIndex
      .map { case (descriptor, idx) =>
        logTabletInfo(descriptor, idx)

        MapRDBTabletInfo(idx, descriptor.getLocations, descriptor.getCondition.asJsonString)
      }
      .map(createReaderFactory)
      .toList
  }

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
      val (supported, unsupported) = filters.partition(isSupportedFilter)
      supportedFilters = supported.toList

      println("supported: ")
      supportedFilters.foreach(print)
      println()

      println("unsupported: ")
      unsupported.foreach(print)
      println()

      unsupported
  }

  override def pushedFilters(): Array[Filter] = {
    println("pushedFilters: ")
    supportedFilters.foreach(print)
    println()

    supportedFilters.toArray
  }

  override def pruneColumns(requiredSchema: StructType): Unit = projections = Some(requiredSchema)

  private def createReaderFactory(tabletInfo: MapRDBTabletInfo) =
    new MapRDBDataPartitionReader(
      tablePath,
      supportedFilters,
      readSchema(),
      tabletInfo,
      hintedIndexes)

  private def isSupportedFilter(filter: Filter): Boolean = filter match {
    case And(a, b) => isSupportedFilter(a) && isSupportedFilter(b)
    case Or(a, b) => isSupportedFilter(a) || isSupportedFilter(b)
    case _: IsNull => true
    case _: IsNotNull => true
    case _: In => true
    case _: StringStartsWith => true
    case EqualTo(_, value) => true // SupportedFilterTypes.isSupportedType(value)
    case LessThan(_, value) => true // SupportedFilterTypes.isSupportedType(value)
    case LessThanOrEqual(_, value) => true // SupportedFilterTypes.isSupportedType(value)
    case GreaterThan(_, value) => true //SupportedFilterTypes.isSupportedType(value)
    case GreaterThanOrEqual(_, value) => true // SupportedFilterTypes.isSupportedType(value)

    case _ => false
  }

  private def logTabletInfo(descriptor: com.mapr.db.TabletInfo, tabletIndex: Int) =
    log.info(
      s"TABLET: $tabletIndex ; " +
        s"PREFERRED LOCATIONS: ${descriptor.getLocations.mkString("[", ",", "]")} ; " +
        s"QUERY: ${descriptor.getCondition.asJsonString()}")
}


object SupportedFilterTypes {

  private val supportedTypes = List[Class[_]](
    classOf[Double],
    classOf[Float],
    classOf[Int],
    classOf[Long],
    classOf[Short],
    classOf[String],
    //    classOf[Timestamp],
    classOf[Boolean],
    classOf[Byte]
  )

  def isSupportedType(value: Any): Boolean = supportedTypes.contains(value.getClass)
}
