package com.github.anicolaspp.spark.sql.writing

import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.v2.writer.{DataWriter, DataWriterFactory, WriterCommitMessage}
import org.apache.spark.sql.types.{DataType, StructType}
import org.ojai.DocumentBuilder
import org.ojai.store.{DocumentStore, DriverManager}

class MapRDBDataWriterFactory(table: String, schema: StructType) extends DataWriterFactory[Row] {

  import com.mapr.db.spark.sql.utils.MapRSqlUtils._

  @transient private lazy val connection = DriverManager.getConnection("ojai:mapr:")

  @transient private lazy val store: DocumentStore = connection.getStore(table)

  private val writtenIds = scala.collection.mutable.ListBuffer.empty[String]

  private val sync = this

  override def createDataWriter(partitionId: Int, attemptNumber: Int): DataWriter[Row] = new DataWriter[Row] with Logging {

    log.info(s"PROCESSING PARTITION ID: $partitionId ; ATTEMPT: $attemptNumber")

    override def write(record: Row): Unit = {

      val doc = schema
        .fields
        .zipWithIndex
        .map { case (field, idx) => (field.name, idx, field.dataType) }
        .foldLeft(connection.newDocumentBuilder()) { case x => foldOp(x, record) }
        .getDocument

      sync.synchronized {
        if (!writtenIds.contains(doc.getIdString)) {
          store.insert(doc)
          writtenIds.append(doc.getIdString)
        }
      }
    }

    private type T = (DocumentBuilder, (String, Int, DataType))

    private def foldOp(t: T, record: Row): DocumentBuilder = t match {
      case (acc, (fieldName, idx, fieldType)) => acc.put(fieldName, convertToDataType(record.get(idx), fieldType))
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
