package com.github.anicolaspp.spark


import com.github.anicolaspp.ojai.OJAIReader
import com.github.anicolaspp.ojai.OJAIReader.Cell
import com.github.anicolaspp.spark.sql.reading.JoinType
import com.mapr.db.spark.utils.MapRSpark
import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

object MapRDB {

  implicit class SessionOps(sparkSession: SparkSession) {

    def loadFromMapRDB(path: String, schema: StructType): DataFrame = {
      sparkSession
        .read
        .format("com.github.anicolaspp.spark.sql.reading.Reader")
        .schema(schema)
        .load(path)
    }

  }

  implicit class DataFrameOps(dataFrame: DataFrame) {

    @Experimental
    def writeToMapRDB(path: String, withTransaction: Boolean = false): Unit =
      if (withTransaction) {
        dataFrame.write
          .format("com.github.anicolaspp.spark.sql.writing.Writer")
          .save(path)

      } else {
        MapRSpark.save(dataFrame, path, "_id", false, false)
      }


    @Experimental
    def joinWithMapRDBTable(table: String,
                            schema: StructType,
                            left: String,
                            right: String,
                            joinType: JoinType)(implicit session: SparkSession): DataFrame = {


      val queryToRight = dataFrame
        .select(left)
        .distinct()
        .persist(StorageLevel.MEMORY_ONLY_2)

      val columnDataType = schema.fields(schema.fieldIndex(right)).dataType

      val documents = queryToRight
        .rdd
        .mapPartitions { partition =>
          if (partition.isEmpty) {
            List.empty.iterator
          } else {

            val partitionCellIterator = partition
              .toIterable
              .par
              .map(row => Cell(row.get(0), columnDataType))
              .toIterator

            OJAIReader.groupedPartitionReader().readFrom(partitionCellIterator, table, schema, right)
          }
        }

      import org.apache.spark.sql.functions._
      import session.implicits._

      val rightDF = session.read.schema(schema).json(documents.toDS)

      dataFrame.join(rightDF, col(left) === col(right), joinType.toString)
    }

    @Experimental
    def joinWithMapRDBTable(maprdbTable: String,
                            schema: StructType,
                            left: String,
                            right: String)(implicit session: SparkSession): DataFrame =
      joinWithMapRDBTable(maprdbTable, schema, left, right, JoinType.inner)
  }

}

