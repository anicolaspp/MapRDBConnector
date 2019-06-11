package com.github.anicolaspp.spark.sql.reading

import java.util

import com.github.anicolaspp.ojai.QueryConditionExtensions._
import com.github.anicolaspp.spark.sql.MapRDBTabletInfo
import com.mapr.db.TabletInfo
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.ojai.Document
import org.ojai.store.{Connection, DocumentStore, DriverManager}

import scala.util.Random

class MapRDBDataSourceMultiReader(schema: StructType,
                                  tablePath: String,
                                  hintedIndexes: List[String],
                                  readersPerTablet: Int)
  extends MapRDBDataSourceReader(schema, tablePath, hintedIndexes) {

  import collection.JavaConversions._
  import scala.collection.JavaConverters._

  override def createDataReaderFactories(): util.List[DataReaderFactory[Row]] =
    if (readersPerTablet == 1) {
      super.createDataReaderFactories()
    } else {
      createReaders
    }

  private def createReaders: List[MapRDBDataPartitionReader] = {

    val tabletInfos = com.mapr.db.MapRDB
      .getTable(tablePath)
      .getTabletInfos
      .par
      .flatMap(splitTablet)

    val factories = tabletInfos.map(createReaderFactory).toList

    log.info(s"CREATING ${factories.length} READERS")

    factories
  }

  private def splitTablet(tablet: TabletInfo) = {

    val connection = DriverManager.getConnection("ojai:mapr:")
    val store: DocumentStore = connection.getStore(tablePath)

    val partition = getPartitionInformation(connection, store, tablet)

    val partitionSize = partition.size

    log.info(s"READER SIZE == $partitionSize")

    val subPartitions = partition
      .grouped(getSubPartitionSize(partitionSize))
      .filter(_.nonEmpty)
      .map(getSubPartitionFrom)
      .map { tabletInfo => getTabletInfoFrom(tabletInfo, tablet)(connection) }
      .toList

    log.info(s"TABLET SPLIT INTO ${subPartitions.size} SUBPARTITIONS")

    subPartitions
  }


  private def getTabletInfoFrom(subPartition: SubPartition, tablet: TabletInfo)(implicit connection: Connection) = {
    val lowerBound = connection.newCondition().field("_id") >= subPartition.startId
    val upperBound = connection.newCondition().field("_id") <= subPartition.endId

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

  private def getSubPartitionFrom(group: List[Document]) =
    SubPartition(group.head.getIdString, group.last.getIdString)

  private def getSubPartitionSize(partitionSize: Int) = (partitionSize / readersPerTablet) + 1

  private def getPartitionInformation(connection: Connection, store: DocumentStore, tablet: TabletInfo) =
    store.find(connection
      .newQuery()
      .where(tablet.getCondition)
      .select("_id")
      .build()).asScala.toList

  private case class SubPartition(startId: String, endId: String)

}
