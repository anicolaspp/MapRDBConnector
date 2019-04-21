package com.github.anicolaspp.spark.sql.reading

import java.util

import com.github.anicolaspp.spark.sql.MapRDBTabletInfo
import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources._
import org.apache.spark.sql.sources.v2.reader.{DataReader, DataReaderFactory, DataSourceReader, SupportsPushDownFilters, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.types.StructType
import org.ojai.store.{DocumentStore, DriverManager}

import scala.util.Random

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

    unsupported
  }

  override def pushedFilters(): Array[Filter] = supportedFilters.toArray

  override def pruneColumns(requiredSchema: StructType): Unit = projections = Some(requiredSchema)

  protected def createReaderFactory(tabletInfo: MapRDBTabletInfo) =
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
    case EqualTo(_, value) => SupportedFilterTypes.isSupportedType(value)
    case LessThan(_, value) => SupportedFilterTypes.isSupportedType(value)
    case LessThanOrEqual(_, value) => SupportedFilterTypes.isSupportedType(value)
    case GreaterThan(_, value) => SupportedFilterTypes.isSupportedType(value)
    case GreaterThanOrEqual(_, value) => SupportedFilterTypes.isSupportedType(value)

    case _ => false
  }

  private def logTabletInfo(descriptor: com.mapr.db.TabletInfo, tabletIndex: Int) =
    log.debug(
      s"TABLET: $tabletIndex ; " +
        s"PREFERRED LOCATIONS: ${descriptor.getLocations.mkString("[", ",", "]")} ; " +
        s"QUERY: ${descriptor.getCondition.asJsonString()}")
}


class MapRDBDataSourceReaderMultiTabletReader(schema: StructType,
                                              tablePath: String,
                                              hintedIndexes: List[String],
                                              readersPerTablet: Int)
  extends MapRDBDataSourceReader(schema, tablePath, hintedIndexes) {

  import collection.JavaConversions._
  import scala.collection.JavaConverters._

  @transient private lazy val connection = DriverManager.getConnection("ojai:mapr:")

  @transient private lazy val store: DocumentStore = connection.getStore(tablePath)


  override def createDataReaderFactories(): util.List[DataReaderFactory[Row]] = {

    import com.github.anicolaspp.ojai.QueryConditionExtensions._
    
    val conditions = com.mapr.db.MapRDB
      .getTable(tablePath)
      .getTabletInfos
      .par
      .flatMap { tablet =>
        val query = connection
          .newQuery()
          .where(tablet.getCondition)
          .select("_id")
          .build()

        val ids = store.find(query)

        val partition = ids.asScala.toList

        val partitionSize = partition.size

        log.info(s"READER SIZE == $partitionSize")

        partition
          .grouped((partitionSize / readersPerTablet) + 1)
          .filter(_.nonEmpty)
          .map(group => (group.head.getIdString, group.last.getIdString))
          .map { range =>

            println(range)

            val lowerBound = connection.newCondition().field("_id") >= range._1
            val upperBound = connection.newCondition().field("_id") <= range._2

            val cond = connection
              .newCondition()
              .and()
              .condition(lowerBound.build())
              .condition(upperBound.build())
              .close()
              .build()
              .asJsonString()

            MapRDBTabletInfo(Random.nextInt(), tablet.getLocations, cond)
          }
      }

    val factories = conditions.map(createReaderFactory).toList

    log.info(s"CREATING ${factories.length} READERS")

    factories
  }
}