package com.github.anicolaspp.spark.sql

import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources._
import org.apache.spark.sql.sources.v2.reader.{DataReader, DataReaderFactory}
import org.apache.spark.sql.types._
import org.ojai.store.Query
import org.ojai.{Value,Document}
import org.ojai.types._
import collection.JavaConverters._
import com.mapr.db.rowcol.DBList

/**
  * Reads data from one particular MapR-DB tablet / region
  *
  * @param table     MapR-DB Table Path
  * @param filters   Filters to be pushed down
  * @param schema    Schema to be pushed down
  * @param locations Preferred location where this task is executed by Spark in order to maintain data locality
  * @param queryJson Extra query to better perform the filtering based on the data for this tablet / region (JSON FORMAT)
  */
class MapRDBDataReaderFactory(table: String,
                              filters: List[Filter],
                              schema: StructType,
                              tabletInfo: MapRDBTabletInfo,
                              hintedIndexes: List[String])
  extends DataReaderFactory[Row] with Logging {

  import org.ojai.store._

  import scala.collection.JavaConverters._

//  import IndexHints._

  @transient private lazy val connection = DriverManager.getConnection("ojai:mapr:")

  @transient private lazy val store: DocumentStore = connection.getStore(table)

  @transient private lazy val documents = {

    val queryResult = store.find(query)

    log.debug(s"OJAI QUERY PLAN: ${queryResult.getQueryPlan}")

    queryResult.asScala.iterator
  }

  private def query: Query = {

    val sparkFiltersQueryCondition = QueryConditionBuilder.buildQueryConditionFrom(filters)(connection)

    val finalQueryConditionString = QueryConditionBuilder.addTabletInfo(tabletInfo.queryJson, sparkFiltersQueryCondition)

    log.debug(s"USING QUERY STRING: $finalQueryConditionString")

    log.debug(s"PROJECTIONS TO PUSH DOWN: $projectionsAsString")


    val query = connection
      .newQuery()
      .where(finalQueryConditionString)
      .select(projectionsNames: _*)
      .build()

    query
  }

  override def preferredLocations(): Array[String] = tabletInfo.locations

  override def createDataReader(): DataReader[Row] = new DataReader[Row] {

    import ParsableDocument._

    override def next(): Boolean = documents.hasNext

    override def get(): Row = {

      val document = documents.next()

      log.debug(document.asJsonString())

      val values = schema.fields
        .foldLeft(List.empty[Any])((xs, field) => getField(document,field) :: xs)
        .reverse

      Row.fromSeq(values)
    }

    override def close(): Unit = {
      store.close()
      connection.close()
    }

    private def createMap(map: java.util.Map[String,Object]): Row = {
      val scalaMap = map.asScala
      val values = scalaMap.keySet
        .foldLeft(List.empty[Any])((xs, field) => getValue(scalaMap(field)) :: xs)
        .reverse
      Row.fromSeq(values)
    }

    private def createArray(array: Array[Object]): Array[Any] = {
      array.map(getValue)
    }

    private def getValue(value: Any): Any = {
      value match {
        case v: OTimestamp => new java.sql.Timestamp(v.getMilliSecond)
        case v: OTime => new java.sql.Timestamp(v.getMilliSecond)
        case v: ODate => v.toDate
        case v: java.util.Map[String,Object] => createMap(v)
        case v: DBList => createArray(v.toArray())
        case v: Any => v
      }
    }

    private def getField(doc: Document, field: StructField): Any = {
      val value = doc.getValue(field.name)
      value.getType match {
        case Value.Type.ARRAY => createArray(value.getList.toArray)
        case Value.Type.BINARY => value.getBinary
        case Value.Type.BOOLEAN => value.getBoolean
        case Value.Type.BYTE => value.getByte
        case Value.Type.DATE => value.getDate.toDate
        case Value.Type.DECIMAL => value.getDecimal
        case Value.Type.DOUBLE => value.getDouble
        case Value.Type.FLOAT => value.getFloat
        case Value.Type.INT => value.getInt
        case Value.Type.INTERVAL => null//TODO: Find the actual type that corresponds to this
        case Value.Type.LONG => value.getLong
        case Value.Type.MAP => createMap(value.getMap)
        case Value.Type.NULL => null
        case Value.Type.SHORT => value.getShort
        case Value.Type.STRING => value.getString
        case Value.Type.TIME => new java.sql.Timestamp(value.getTime.getMilliSecond)
        case Value.Type.TIMESTAMP => new java.sql.Timestamp(value.getTimestamp.getMilliSecond)
      }
    }
  }

  override protected def logName: String = super.logName + s"===== TABLE ${tabletInfo.internalId}"
  
  private def projectionsAsString: String =
    schema
      .fields
      .foldLeft(List.empty[(String, DataType)])((xs, field) => (field.name, field.dataType) :: xs)
      .mkString("[", ",", "]")

  private def projectionsNames: Array[String] = schema.fields.map(_.name)
}

object IndexHints {

  implicit class HintedQuery(query: Query) {
    def addHints(hints: List[String], readerId: Int): Query =
      if (readerId == 2) {
        hints.foldLeft(query)((q, hint) => q.setOption("ojai.mapr.query.hint-using-index", hint))
      } else {
        query
      }
  }

}
