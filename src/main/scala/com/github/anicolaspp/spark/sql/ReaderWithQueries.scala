package com.github.anicolaspp.spark.sql

import java.util

import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.v2.reader.{DataReaderFactory, DataSourceReader}
import org.apache.spark.sql.sources.v2.{DataSourceOptions, ReadSupportWithSchema}
import org.apache.spark.sql.types.StructType

class ReaderWithQueries extends ReadSupportWithSchema {
  override def createReader(schema: StructType, options: DataSourceOptions): DataSourceReader = new DataSourceReader {

    import collection.JavaConversions._

    override def readSchema(): StructType = schema

    override def createDataReaderFactories(): util.List[DataReaderFactory[Row]] = {

      val tablePath = options.get("path").get()
      val queries = options.get("queries").get()
      
      com.mapr.db.MapRDB
        .getTable(tablePath)
        .getTabletInfos
        .zipWithIndex
        .map { case (descriptor, idx) =>

          val tableQuery = "{\"$and\":[" + descriptor.getCondition.asJsonString() + "," +
            "{\"$or\":[" + queries + "]}]}"

          MapRDBTabletInfo(idx, descriptor.getLocations, tableQuery)
        }
        .map(tabletInfo => new MapRDBDataPartitionReaderNoFilters(tablePath, readSchema(), tabletInfo))
        .toList
    }
  }
}

