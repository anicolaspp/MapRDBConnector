package com.github.anicolaspp.ojai

import com.github.anicolaspp.ojai.OJAISparkPartitionReader.Cell
import org.apache.spark.sql.types.StructType

/**
  * PartitionQueryRunner reads the MapR-DB data that matches with certain rows.
  *
  * Each Spark executor has an instance of PartitionQueryRunner.
  */
private[ojai] class GroupedPartitionQueryRunner(querySize: Int) extends OJAISparkPartitionReader {

  import com.github.anicolaspp.concurrent.ConcurrentContext.Implicits._
  import com.mapr.db.spark.sql.utils.MapRSqlUtils._
  import org.ojai.store._

  import collection.JavaConversions._
  import scala.collection.JavaConverters._

  /**
    * Reads MapR-DB records that match with the data in a given partition.
    *
    * @param partition Contains the records used to match the data to be read from MapR-DB.
    * @param table     MapR-DB table to read from.
    * @param schema    Schema to be enforced over the MapR-DB data after the read.
    * @param right     Column to be used for MapR-DB query.
    * @return Iterator that contains all records from MapR-DB that match with the data of the given partition.
    */
  def readFrom(partition: Iterator[Cell],
               table: String,
               schema: StructType,
               right: String): Iterator[String] = {

    val connection = DriverManager.getConnection("ojai:mapr:")
    val store = connection.getStore(table)

    val parallelRunningQueries = partition
      .map(cell => convertToDataType(cell.value, cell.dataType))
      .grouped(querySize)
      .map(group => connection.newCondition().in(right, group).build())
      .map(cond =>
        connection
          .newQuery()
          .where(cond) // Filters push down. Secondary indexes kick in here.
          .select(schema.fields.map(_.name): _*) // Projections push down.
          .build()
      )
      .map(query => store.find(query).asScala.map(_.asJsonString()).async)

    parallelRunningQueries.awaitSliding().flatten
  }
}