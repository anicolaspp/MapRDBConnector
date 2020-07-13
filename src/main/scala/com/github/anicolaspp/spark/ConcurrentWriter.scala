package com.github.anicolaspp.spark

import java.io.IOException

import com.github.anicolaspp.concurrent.ConcurrentContext
import com.mapr.db.spark.MapRDBSpark
import org.apache.spark.sql.{DataFrame, Row}
import org.ojai.store.{Connection, DocumentStore, DriverManager}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

object ConcurrentWriter {

  implicit class MultiThreadDFWriter(df: DataFrame) {
    def writeToMapRDB(path: String, threadsPerPartition: Int): Unit = {
      df.foreachPartition { partition =>
        tryWriting(path, threadsPerPartition, partition)
          .map { case (connection, store) =>
            store.close()
            connection.close()
          }
      }
    }
  }

  private def tryWriting(path: String, threadsPerPartition: Int, partition: Iterator[Row]): Option[(Connection, DocumentStore)] =
    getConnectionAndStore(path) match {
      case None => throw new IOException(s"Impossible to access the table $path")
      case Some((connection, store)) =>
        implicit val ec: ExecutionContext = ConcurrentContext.apply(threadsPerPartition).ec

        val tasks = partition.map { row =>
          val doc = MapRDBSpark.rowToDoc(row)

          Future(store.insertOrReplace(doc.getDoc))
        }

        val partitionWritingTask = Future.sequence(tasks)

        Await.ready(partitionWritingTask, Duration.Inf)

        Some(connection, store)
    }

  private def getConnectionAndStore(tablePath: String): Option[(Connection, DocumentStore)] = Try {
    DriverManager.getConnection("ojai:mapr:")
  }
    .flatMap { connection =>
      Try {
        connection.getStore(tablePath)
      } match {
        case Failure(exception) =>
          connection.close()
          Failure(exception)
        case Success(value) => Success((connection, value))
      }
    }
    .toOption
}