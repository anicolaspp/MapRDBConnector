package com.github.anicolaspp.spark.sql

import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.v2.reader.{DataReader, DataReaderFactory}
import org.apache.spark.sql.sources.{EqualTo, Filter, GreaterThan}
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

  private def createFilterCondition(filters: List[Filter]): QueryCondition = {

    println("FILTERS: " + filters)

    filters.foldLeft(connection.newCondition().and()) { (condition, filter) =>

      filter match {
        case EqualTo(field, value: String) => condition.is(field, QueryCondition.Op.EQUAL, value)
        case EqualTo(field, value: Int) => condition.is(field, QueryCondition.Op.EQUAL, value)

        case GreaterThan(field, value: String) => condition.is(field, QueryCondition.Op.GREATER, value)
        case GreaterThan(field, value: Int) => condition.is(field, QueryCondition.Op.GREATER, value)

      }

    }
      .close()
      .build()
  }

  private def query = {

    val condition = createFilterCondition(filters)

    println("CONDITION: " + condition.toString)

    println("SCHEMA: " + schema)

    val query = connection
      .newQuery()
      .select(schema.fields.map(_.name): _*)
      .setOption("ojai.mapr.query.hint-using-index", "uid_idx")
      .where(condition)
      .build()

    query
  }

  override def createDataReader(): DataReader[Row] = new DataReader[Row] {
    override def next(): Boolean = documents.hasNext

    override def get(): Row = {

      val document = documents.next()

      println(document)

      val values = schema.fields.map(_.name).foldLeft(List.empty[String])((xs, name) => document.getString(name) :: xs).reverse

      Row.fromSeq(values)
    }

    override def close(): Unit = {
      store.close()
      connection.close()
    }
  }
}
