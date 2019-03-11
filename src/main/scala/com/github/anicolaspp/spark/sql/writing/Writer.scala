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












