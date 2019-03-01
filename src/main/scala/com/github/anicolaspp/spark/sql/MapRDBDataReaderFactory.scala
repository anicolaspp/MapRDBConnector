package com.github.anicolaspp.spark.sql

import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources._
import org.apache.spark.sql.sources.v2.reader.{DataReader, DataReaderFactory}
import org.apache.spark.sql.types.{DataType, StructType}

class MapRDBDataReaderFactory(table: String, filters: List[Filter], schema: StructType)
  extends DataReaderFactory[Row] with Logging {

  import org.ojai.store._

  import scala.collection.JavaConverters._

  @transient private lazy val connection = DriverManager.getConnection("ojai:mapr:")

  @transient private lazy val store: DocumentStore = connection.getStore(table)

  @transient private lazy val documents = {

    val queryResult = store.find(query)

    log.trace(s"OJAI QUERY PLAN: ${queryResult.getQueryPlan}")

    queryResult.asScala.iterator
  }
  
  private def query: Query = {

    val queryCondition = QueryConditionBuilder.buildQueryConditionFrom(filters)(connection)

    log.trace(s"PROJECTIONS TO PUSH DOWN: $projectionsAsString")

    val query = connection
      .newQuery()
      .where(queryCondition)
      .select(projectionsNames: _*)
      .build()

    query
  }

  def projectionsAsString: String =
    schema
      .fields
      .foldLeft(List.empty[(String, DataType)])((xs, field) => (field.name, field.dataType) :: xs)
      .mkString("[", ",", "]")

  def projectionsNames: Array[String] = schema.fields.map(_.name)

  override def createDataReader(): DataReader[Row] = new DataReader[Row] {

    override def next(): Boolean = documents.hasNext

    override def get(): Row = {

      val document = documents.next()

      log.trace(document.asJsonString())

      val values = projectionsNames
        .foldLeft(List.empty[String])((xs, name) => document.getString(name) :: xs)
        .reverse

      Row.fromSeq(values)
    }

    override def close(): Unit = {
      store.close()
      connection.close()
    }
  }
}

