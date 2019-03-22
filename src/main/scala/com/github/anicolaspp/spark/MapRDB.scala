package com.github.anicolaspp.spark


import com.github.anicolaspp.spark.sql.reading.JoinType
import com.mapr.db.spark.utils.MapRSpark
import org.apache.spark.annotation.{DeveloperApi, Experimental}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.concurrent.{Await, Future}

object MapRDB {

  implicit class ExtendedSession(sparkSession: SparkSession) {

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
    def joinWithMapRDBTable(maprdbTable: String,
                            schema: StructType,
                            left: String,
                            right: String,
                            joinType: JoinType)(implicit session: SparkSession): DataFrame = {
      import org.ojai.store._

      import collection.JavaConversions._
      import scala.collection.JavaConverters._

      val queryToRight = dataFrame
        .cache()
        .select(left)
        .distinct()
        .repartition(200)

      import scala.concurrent.ExecutionContext.Implicits.global

      val documents = queryToRight
        .rdd
        .mapPartitions { partition =>

          if (partition.isEmpty) {
             List.empty.iterator
          } else {

            val connection = DriverManager.getConnection("ojai:mapr:")
            val store = connection.getStore(maprdbTable)

            val parallelRunningQueries = partition
              .map(row => com.mapr.db.spark.sql.utils.MapRSqlUtils.convertToDataType(row.get(0), schema.fields(schema.fieldIndex(right)).dataType))
              .map(v => connection.newCondition().in(right, List(v)).build())
              .map { cond =>
                connection
                  .newQuery()
                  .where(cond)
                  .select(schema.fields.map(_.name): _*)
                  .build()
              }
              .map(query => Future { store.find(query).asScala.map(_.asJsonString()) })

            val aggregatedRunningResult = Future.fold(parallelRunningQueries)(List.empty[String])((a, b) => a ++ b.toList)

            import scala.concurrent.duration.Duration._

            Await.result(aggregatedRunningResult, Inf).iterator
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