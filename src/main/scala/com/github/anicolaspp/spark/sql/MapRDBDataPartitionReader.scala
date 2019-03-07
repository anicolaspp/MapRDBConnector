package com.github.anicolaspp.spark.sql

import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources._
import org.apache.spark.sql.sources.v2.reader.{DataReader, DataReaderFactory}
import org.apache.spark.sql.types._

/**
  * Reads data from one particular MapR-DB tablet / region
  *
  * @param table      MapR-DB Table Path
  * @param filters    Filters to be pushed down
  * @param schema     Schema to be pushed down
  * @param tabletInfo Specific information of the tablet being used by this reader
  */
class MapRDBDataPartitionReader(table: String,
                                filters: List[Filter],
                                schema: StructType,
                                tabletInfo: MapRDBTabletInfo,
                                hintedIndexes: List[String])
  extends DataReaderFactory[Row] with Logging {

  import org.ojai.store._

  import scala.collection.JavaConverters._

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

    override def next(): Boolean = documents.hasNext

    override def get(): Row = {

      val document = documents.next()

      log.debug(document.asJsonString())

      val values = schema.fields
        .foldLeft(List.empty[Any])((xs, field) => ParsableDocument.ParsableDocument(document).get(field) :: xs)
        .reverse

      Row.fromSeq(values)
    }

    override def close(): Unit = {
      store.close()
      connection.close()
    }

  }

  override protected def logName: String = super.logName + s" ===== TABLET: ${tabletInfo.internalId}"

  private def projectionsAsString: String =
    schema
      .fields
      .foldLeft(List.empty[(String, DataType)])((xs, field) => (field.name, field.dataType) :: xs)
      .mkString("[", ",", "]")

  private def projectionsNames: Array[String] = schema.fields.map(_.name)
}
