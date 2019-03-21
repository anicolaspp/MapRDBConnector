package com.github.anicolaspp.spark.sql.writing

import org.ojai.store.{DocumentStore, DriverManager}

object MapRDBCleaner {

  def clean(ids: Set[String], table: String): Unit = {

    val connection = DriverManager.getConnection("ojai:mapr:")

    val store: DocumentStore = connection.getStore(table)

    ids.foreach(store.delete)
  }
}
