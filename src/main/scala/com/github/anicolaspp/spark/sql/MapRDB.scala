package com.github.anicolaspp.spark.sql

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}


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
      import org.ojai.store._

      import collection.JavaConversions._
      import scala.collection.JavaConverters._

      val queryToRight = dataFrame
        .cache()
        .select(left)
        .distinct()
        .repartition(200)

      val documents = queryToRight
        .rdd
        .mapPartitions { partition =>

          if (partition.isEmpty) {
            List.empty.iterator
          } else {

            val leftValues = partition.map { row =>
              com.mapr.db.spark.sql.utils.MapRSqlUtils.convertToDataType(row.get(0), schema.fields(schema.fieldIndex(right)).dataType)
            }

            val connection = DriverManager.getConnection("ojai:mapr:")
            val store = connection.getStore(maprdbTable)

            val inCondition = connection
              .newCondition()
              .in(right, leftValues.toList)
              .build()

            println(inCondition.asJsonString())

            val query = connection
              .newQuery()
              .where(inCondition)
              .select(schema.fields.map(_.name): _*)
              .build()

            val result = store.find(query).asScala.map(_.asJsonString()).iterator

            store.close()
            connection.close()

            result
          }
        }

//      documents.take(10).foreach(println)

      import session.implicits._

      val rightDF = session.read.schema(schema).json(documents.toDS)

      dataFrame.join(rightDF, col(left) === col(right))
    }
  }

}