package com.github.anicolaspp.spark.sql.writing

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.writer.{DataSourceWriter, DataWriterFactory, WriterCommitMessage}
import org.apache.spark.sql.types.StructType

class MapRDBDataSourceWriter(table: String, schema: StructType) extends DataSourceWriter with Logging {

  private var globallyCommittedIds = List.empty[String]

  override def createWriterFactory(): DataWriterFactory[InternalRow] = new MapRDBDataWriterFactory(table, schema)

  override def commit(messages: Array[WriterCommitMessage]): Unit = {

    val ids = messages.foldLeft(Set.empty[String]) { case (acc, CommittedIds(partitionId, partitionIds)) =>
      log.info(s"PARTITION $partitionId HAS BEEN CONFIRMED BY DRIVER")

      acc ++ partitionIds
    }

    // Let's make sure this is thread-safe
    globallyCommittedIds = this.synchronized {
      globallyCommittedIds ++ ids
    }

  }

  override def abort(messages: Array[WriterCommitMessage]): Unit = {
    log.info("JOB BEING ABORTED")
    log.info("JOB CLEANING UP")

    MapRDBCleaner.clean(globallyCommittedIds.toSet, table)
  }
}
