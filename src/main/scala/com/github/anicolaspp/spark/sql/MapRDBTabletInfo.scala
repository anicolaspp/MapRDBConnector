package com.github.anicolaspp.spark.sql

case class MapRDBTabletInfo private[sql](internalId: Int, locations: Array[String], queryJson: String)
