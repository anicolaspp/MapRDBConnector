package com.github.anicolaspp.spark.sql

import org.apache.spark.sql.Row
import org.apache.spark.sql.sources._
import org.apache.spark.sql.sources.v2.reader.{DataReader, DataReaderFactory}
import org.apache.spark.sql.types.StructType

class MapRDBDataReaderFactory(table: String, filters: List[Filter], schema: StructType)
  extends DataReaderFactory[Row] {

  import org.ojai.store._

  import scala.collection.JavaConverters._

  @transient private lazy val connection = DriverManager.getConnection("ojai:mapr:")

  @transient private lazy val store: DocumentStore = connection.getStore(table)

  @transient private lazy val documents = {

    val queryResult = store.find(query)

    println(s"QUERY PLAN: ${queryResult.getQueryPlan}")

    queryResult.asScala.iterator
  }


  private def query: Query = {

    val queryCondition = QueryConditionBuilder.buildQueryConditionFrom(filters)(connection)

    println(s"PROJECTIONS: $schema")

    val query = connection
      .newQuery()
      .where(queryCondition)
      .select(schema.fields.map(_.name): _*)
      .build()

    query
  }

  override def createDataReader(): DataReader[Row] = new DataReader[Row] {

    override def next(): Boolean = documents.hasNext

    override def get(): Row = {

      val document = documents.next()

      println(document)

      val values = schema
        .fields
        .map(_.name)
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

