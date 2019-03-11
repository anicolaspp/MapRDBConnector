package com.github.anicolaspp.spark.sql.writing

import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.v2.writer.{DataWriter, WriterCommitMessage}

class MapRDBDataWriter extends DataWriter[Row] with Logging {
  override def write(record: Row): Unit = ???

  override def commit(): WriterCommitMessage = ???

  override def abort(): Unit = ???
}
