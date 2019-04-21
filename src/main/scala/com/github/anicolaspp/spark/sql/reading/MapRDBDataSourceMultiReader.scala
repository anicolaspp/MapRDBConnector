package com.github.anicolaspp.spark.sql.reading

import java.util

import com.github.anicolaspp.spark.sql.MapRDBTabletInfo
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.v2.reader.DataReaderFactory
import org.apache.spark.sql.types.StructType
import org.ojai.store.{DocumentStore, DriverManager}

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
    import com.github.anicolaspp.ojai.QueryConditionExtensions._

    val connection = DriverManager.getConnection("ojai:mapr:")
    val store: DocumentStore = connection.getStore(tablePath)

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
