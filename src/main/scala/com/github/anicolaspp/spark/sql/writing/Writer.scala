package com.github.anicolaspp.spark.sql.writing

import java.util.Optional

import org.apache.spark.internal.Logging
import org.apache.spark.sql.sources.v2.writer.{DataSourceWriter, DataWriter, DataWriterFactory, WriterCommitMessage}
import org.apache.spark.sql.sources.v2.{DataSourceOptions, WriteSupport}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SaveMode}
import org.ojai.store.{DocumentStore, DriverManager}


class Writer extends WriteSupport with Logging {

  override def createWriter(jobId: String, schema: StructType, mode: SaveMode, options: DataSourceOptions): Optional[DataSourceWriter] = {

    val tablePath = options.get("path").get()

    log.info(s"TABLE PATH BEING USED: $tablePath")

    java.util.Optional.of(new MapRDBDataSourceWriter(tablePath, schema))
  }
}

class MapRDBDataSourceWriter(table: String, schema: StructType) extends DataSourceWriter with Logging {

  private var globallyCommittedIds = List.empty[String]

  override def createWriterFactory(): DataWriterFactory[Row] = new MapRDBDataWriterFactory(table, schema)

  override def commit(messages: Array[WriterCommitMessage]): Unit = {

    val ids = messages.foldLeft(Set.empty[String]) { case (acc, CommittedIds(partitionId, ids)) =>
      log.info(s"PARTITION $partitionId HAS BEEN CONFIRMED BY DRIVER")

      acc ++ ids
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

class MapRDBDataWriterFactory(table: String, schema: StructType) extends DataWriterFactory[Row] {

  override def createDataWriter(partitionId: Int, attemptNumber: Int): DataWriter[Row] = new DataWriter[Row] with Logging {

    @transient private lazy val connection = DriverManager.getConnection("ojai:mapr:")

    @transient private lazy val store: DocumentStore = connection.getStore(table)

    log.info(s"PROCESSING PARTITION ID: $partitionId ; ATTEMPT: $attemptNumber")

    private val writtenIds = scala.collection.mutable.ListBuffer.empty[String]

    override def write(record: Row): Unit = {

      val doc = schema
        .fields
        .map(field => (field.name, schema.fieldIndex(field.name)))
        .foldLeft(connection.newDocumentBuilder()) { case (acc, (name, idx)) => acc.put(name, record.getString(idx)) }
        .getDocument

      store.insert(doc)

      writtenIds.append(doc.getIdString)
    }

    override def commit(): WriterCommitMessage = {
      log.info(s"PARTITION $partitionId COMMITTED AFTER ATTEMPT $attemptNumber")

      CommittedIds(partitionId, writtenIds.toSet)
    }

    override def abort(): Unit = {
      log.info(s"PARTITION $partitionId ABORTED AFTER ATTEMPT $attemptNumber")

      MapRDBCleaner.clean(writtenIds.toSet, table)

      log.info(s"PARTITION $partitionId CLEANED UP")
    }
  }
}

class MapRDBDataWriter extends DataWriter[Row] with Logging {
  override def write(record: Row): Unit = ???

  override def commit(): WriterCommitMessage = ???

  override def abort(): Unit = ???
}


object MapRDBCleaner {

  def clean(ids: Set[String], table: String): Unit = {

    val connection = DriverManager.getConnection("ojai:mapr:")

    val store: DocumentStore = connection.getStore(table)

    ids.foreach(store.delete)
  }
}


case class CommittedIds(partitionId: Int, ids: Set[String]) extends WriterCommitMessage
