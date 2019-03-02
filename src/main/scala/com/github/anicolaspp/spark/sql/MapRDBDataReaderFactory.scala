package com.github.anicolaspp.spark.sql

import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources._
import org.apache.spark.sql.sources.v2.reader.{DataReader, DataReaderFactory}
import org.apache.spark.sql.types._
import org.ojai.{Document,Value}

class MapRDBDataReaderFactory(table: String,
                              filters: List[Filter],
                              schema: StructType,
                              locations: Array[String],
                              queryJson: String) extends DataReaderFactory[Row] with Logging {

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

    val sparkFiltersQueryCondition = QueryConditionBuilder.buildQueryConditionFrom(filters)(connection)

    val finalQueryConditionString = QueryConditionBuilder.addTabletInfo(queryJson, sparkFiltersQueryCondition)

    log.trace(s"Using query string: $finalQueryConditionString")

    log.trace(s"PROJECTIONS TO PUSH DOWN: $projectionsAsString")

    val query = connection
      .newQuery()
      .where(finalQueryConditionString)
      .select(projectionsNames: _*)
      .build()

    query
  }

  override def preferredLocations(): Array[String] = locations

  override def createDataReader(): DataReader[Row] = new DataReader[Row] {

    override def next(): Boolean = documents.hasNext

    override def get(): Row = {

      val document = documents.next()

      log.trace(document.asJsonString())

      val values = schema.fields
        .foldLeft(List.empty[Any])((xs, field) => getAnyFromDocument(document, field.name, field.dataType) :: xs)
        .reverse

      Row.fromSeq(values)
    }

    override def close(): Unit = {
      store.close()
      connection.close()
    }
  }

  private def getAnyFromDocument(doc: Document, name: String, dataType: DataType ) = {
    dataType match {
      case BinaryType => doc.getBinary(name)
      case BooleanType => doc.getBooleanObj(name)
      case ByteType => doc.getByteObj(name)
      case DateType => doc.getDate(name)
      case _:DecimalType => doc.getDecimal(name)
      case DoubleType => doc.getDoubleObj(name)
      case FloatType => doc.getFloatObj(name)
      case IntegerType => doc.getIntObj(name)
      case ArrayType(_,_) => doc.getList(name) //TODO Need to check what happens in the regular connector
      case LongType => doc.getLongObj(name)
      case StructType(_) => doc.getMap(name) //TODO This might need to be done recursively based on the values in the StructType
      case ShortType => doc.getShortObj(name)
      case StringType => doc.getString(name)
      case TimestampType => {
        val valObj = doc.getValue(name)
        if(valObj.getType == Value.TYPE_CODE_TIME) {
          new java.sql.Timestamp(valObj.getTime.getMilliSecond)
        } else {
          new java.sql.Timestamp(valObj.getTimestamp.getMilliSecond)
        }
      }
        //TODO this will probably not automatically convert from OInterval to CalendarIntervalType
        // It seems that this will need to be convert to a correct string representation
      case CalendarIntervalType => doc.getInterval(name)
      case _ => throw new IllegalArgumentException(s"Unknown type ${dataType} for field ${name}")
    }
  }

  private def projectionsAsString: String =
    schema
      .fields
      .foldLeft(List.empty[(String, DataType)])((xs, field) => (field.name, field.dataType) :: xs)
      .mkString("[", ",", "]")

  private def projectionsNames: Array[String] = schema.fields.map(_.name)
}

