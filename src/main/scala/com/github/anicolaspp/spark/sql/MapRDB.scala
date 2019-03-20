package com.github.anicolaspp.spark.sql

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.ojai.store.DriverManager


object MapRDB {

  implicit class ExtendedSession(sparkSession: SparkSession) {

    def loadFromMapRDB(path: String, schema: StructType): DataFrame = {
      sparkSession
        .read
        .format("com.github.anicolaspp.spark.sql.Reader")
        .schema(schema)
        .load(path)
    }
  }


  implicit class DataFrameOps(dataFrame: DataFrame) {
    def joinWithMapRDBTable(maprdbTable: String, schema: StructType, left: String, right: String)(session: SparkSession): DataFrame = {
      import org.apache.spark.sql.functions._
      import collection.JavaConversions._

      val queryToRight = dataFrame
        .cache()
        .select(left)
        .withColumnRenamed(left, "a")
        .distinct()

      val queries = queryToRight.rdd.mapPartitions { partition =>

        if (partition.isEmpty) List.empty.iterator else {
          val connection = DriverManager.getConnection("ojai:mapr:")

          val values = partition.map { row =>
            val (idx, dtype) = (schema.fieldIndex(right), schema.fields(schema.fieldIndex(right)).dataType)

            val x = com.mapr.db.spark.sql.utils.MapRSqlUtils.convertToDataType(row.get(0), dtype)

            x
          }

          val inCondition = connection
            .newCondition()
            .in(right, values.toList)
            .build()
            .asJsonString()

          List(inCondition).iterator
        }
      }
        .collect()
        .filterNot(_ == "false") // this is a query per partition => no that many so we should collect without issues

      println(s"THERE ARE: ${queries.length} QUERIES TO BE EXECUTED")

      val rightDF = session
        .read
        .format("com.github.anicolaspp.spark.sql.ReaderWithQueries")
        .option("queries", queries.mkString(","))
        .schema(schema)
        .load(maprdbTable)

      dataFrame.join(rightDF, col(left) === col(right))
    }
  }

}


object OJAILoader {
  def loadRowsWithIds(ids: DataFrame, tablePath: String, schema: StructType)(implicit session: SparkSession): DataFrame = {

    import session.implicits._

    val jsonRows = ids.mapPartitions { partition =>
      val ojaiConnection = DriverManager.getConnection("ojai:mapr:")
      val store = ojaiConnection.getStore(tablePath)

      val outList = partition.foldLeft(List.empty[String]) { (result, nextId) =>

        val documentWithId = store.findById(nextId.get(0).toString)

        documentWithId.asJsonString :: result
      }

      store.close()
      ojaiConnection.close()

      outList.iterator
    }.rdd


    session.read.schema(schema).json(jsonRows.toDS())
  }
}