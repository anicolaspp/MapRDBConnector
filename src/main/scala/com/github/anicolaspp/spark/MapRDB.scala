package com.github.anicolaspp.spark


import com.github.anicolaspp.ojai.OJAISparkPartitionReader
import com.github.anicolaspp.ojai.OJAISparkPartitionReader.Cell
import com.github.anicolaspp.spark.sql.reading.JoinType
import com.mapr.db.spark.utils.MapRSpark
import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

object MapRDB {
  
  implicit class SessionOps(sparkSession: SparkSession) {

    def loadFromMapRDB(path: String, schema: StructType, many: Int = 1): DataFrame = {
      sparkSession
        .read
        .format("com.github.anicolaspp.spark.sql.reading.Reader")
        .schema(schema)
        .option("readers", many)
        .load(path)
    }
  }

  implicit class DataFrameOps(dataFrame: DataFrame) {

    @Experimental
    def writeToMapRDB(path: String, withTransaction: Boolean = false): Unit =
      if (withTransaction) {
        dataFrame
          .write
          .format("com.github.anicolaspp.spark.sql.writing.Writer")
          .save(path)

      } else {
        MapRSpark.save(dataFrame, path, "_id", createTable = false, bulkInsert = false)
      }


    @Experimental
    def joinWithMapRDBTable(table: String,
                            schema: StructType,
                            left: String,
                            right: String,
                            joinType: JoinType,
                            concurrentQueries: Int = 20)(implicit session: SparkSession): DataFrame = {

      val columnDataType = schema.fields(schema.fieldIndex(right)).dataType

      val documents = dataFrame
        .select(left)
        .distinct()
        .rdd
        .mapPartitions { partition =>
          if (partition.isEmpty) {
            List.empty.iterator
          } else {

            val partitionCellIterator = partition.map(row => Cell(row.get(0), columnDataType))

            OJAISparkPartitionReader.groupedPartitionReader(concurrentQueries).readFrom(partitionCellIterator, table, schema, right)
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

