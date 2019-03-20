package com.github.anicolaspp.spark.sql

import org.apache.spark.sql.types.StructType

private [anicolaspp] class MapRDBDataPartitionReaderNoFilters(table: String, schema: StructType, tabletInfo: MapRDBTabletInfo)
  extends MapRDBDataPartitionReader(table, List.empty, schema, tabletInfo, List.empty)
