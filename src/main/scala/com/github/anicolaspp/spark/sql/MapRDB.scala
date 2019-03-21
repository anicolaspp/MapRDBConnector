package com.github.anicolaspp.spark.sql

import java.util.function.Consumer
import java.util.stream.Collectors

import com.mapr.db.spark.MapRDBSpark
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.internal.Logging
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.ojai.DocumentReader


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


  implicit class DataFrameOps(dataFrame: DataFrame) extends Logging {

    @DeveloperApi
    def joinWithMapRDBTable(maprdbTable: String, schema: StructType, left: String, right: String)(session: SparkSession): DataFrame = {

      import org.apache.spark.sql.functions._
      import org.ojai.store._

      import collection.JavaConversions._
      import scala.collection.JavaConverters._

      val queryToRight = dataFrame
        .cache()
        .select(left)
        .distinct()

      val documents = queryToRight
        .rdd
        .mapPartitions { partition =>

          if (partition.isEmpty) {
            List.empty.iterator
          } else {

            val connection = DriverManager.getConnection("ojai:mapr:")
            val store = connection.getStore(maprdbTable)

            val m = partition.map(row =>com.mapr.db.spark.sql.utils.MapRSqlUtils.convertToDataType(row.get(0), schema.fields(schema.fieldIndex(right)).dataType) )
              .grouped(10)
              .map(v => connection.newCondition().in(right, v).build())
              .map(cond => connection
                .newQuery()
                .where(cond)
                .select(schema.fields.map(_.name): _*)
                .build())
              .flatMap(query => store.find(query).asScala.map(_.asJsonString()))

            store.close()
            connection.close()

            m
          }
        }
      
      import session.implicits._

      val rightDF = session.read.schema(schema).json(documents.toDS)

      dataFrame.join(rightDF, col(left) === col(right))
    }
  }

}