package com.github.anicolaspp.spark.sql.writing

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.writer.{DataWriter, DataWriterFactory, WriterCommitMessage}
import org.apache.spark.sql.types.{DataType, StructType}
import org.ojai.DocumentBuilder
import org.ojai.store.{DocumentStore, DriverManager}

class MapRDBDataWriterFactory(table: String, schema: StructType) extends DataWriterFactory[InternalRow] {

  import com.mapr.db.spark.sql.utils.MapRSqlUtils._

  @transient private lazy val connection = DriverManager.getConnection("ojai:mapr:")

  @transient private lazy val store: DocumentStore = connection.getStore(table)

  private val writtenIds = scala.collection.mutable.ListBuffer.empty[String]

  private val sync = this
  
  override def createDataWriter(partitionId: Int, taskId: Long, epochId: Long): DataWriter[InternalRow] = new DataWriter[InternalRow] with Logging {

      log.info(s"PROCESSING PARTITION ID: $partitionId ; TASK ID: $taskId")

      override def write(record: InternalRow): Unit = {

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

      private def foldOp(t: T, record: InternalRow): DocumentBuilder = t match {
        case (acc, (fieldName, idx, fieldType)) => acc.put(fieldName, convertToDataType(record.get(idx, fieldType), fieldType))
      }

      override def commit(): WriterCommitMessage = CommittedIds(partitionId, writtenIds.toSet)

      override def abort(): Unit = MapRDBCleaner.clean(writtenIds.toSet, table)

    }
}
