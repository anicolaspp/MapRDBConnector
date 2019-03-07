package com.github.anicolaspp.spark.sql

/**
  *
  * @param internalId Internal tablet identifier
  * @param locations  Preferred location where this task is executed by Spark in order to maintain data locality
  * @param queryJson  Extra query to better perform the filtering based on the data for this tablet / region (JSON FORMAT)
  */
case class MapRDBTabletInfo private[sql](internalId: Int, locations: Array[String], queryJson: String)
