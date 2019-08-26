package com.github.anicolaspp.spark


import com.github.anicolaspp.ojai.OJAISparkPartitionReader
import com.github.anicolaspp.ojai.OJAISparkPartitionReader.Cell
import com.github.anicolaspp.spark.sql.reading.JoinType
import com.mapr.db.spark.utils.MapRSpark
import org.apache.hadoop.fs.{FileStatus, Path, PathFilter}
import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.ojai.store.DriverManager

object MapRDB {

  implicit class SessionOps(sparkSession: SparkSession) {

    def loadFromMapRDB(path: String, schema: StructType, many: Int = 1): DataFrame = {

      if (path.contains("*")) {
        val paths = expand(path)

        if (paths.nonEmpty) {
          loadUnionFromMapRDB(paths: _*)(schema, many)
        } else {
          sparkSession.emptyDataFrame
        }
      } else {
        sparkSession
          .read
          .format("com.github.anicolaspp.spark.sql.reading.Reader")
          .schema(schema)
          .option("readers", many)
          .load(path)
      }
    }

    private def loadUnionFromMapRDB(paths: String*)(schema: StructType, many: Int = 1): DataFrame =
      paths
        .map(path => loadFromMapRDB(path, schema, many))
        .reduce { (a, b) =>
          if (a.schema != b.schema) {
            throw new Exception(s"Table Schema does not match. ${a.schema} != ${b.schema}")
          } else {
            a.union(b)
          }
        }

    private def expand(path: String): Seq[String] =
      FileOps.fs
        .globStatus(new Path(path), FileOps.dbPathFilter)
        .map(_.getPath.toString)
  }

  implicit class DataFrameOps(dataFrame: DataFrame) {

    def writeToMapRDB(path: String, withTransaction: Boolean = false): Unit =
      if (withTransaction) {
        dataFrame.write
          .format("com.github.anicolaspp.spark.sql.writing.Writer")
          .save(path)

      } else {
        MapRSpark.save(dataFrame, path, "_id", false, false)
      }

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

    def joinWithMapRDBTable(maprdbTable: String,
                            schema: StructType,
                            left: String,
                            right: String)(implicit session: SparkSession): DataFrame =
      joinWithMapRDBTable(maprdbTable, schema, left, right, JoinType.inner)
  }

}

object FileOps {

  import org.apache.hadoop.conf.Configuration
  import org.apache.hadoop.fs.{FileSystem, Path}

  lazy val fs: FileSystem = {
    val conf = new Configuration()

    println(s"File System Configuration: $conf")

    FileSystem.get(conf)
  }

  lazy val dbPathFilter: PathFilter = new PathFilter {
    private lazy val connection = DriverManager.getConnection("ojai:mapr:")

    override def accept(path: Path): Boolean = connection.storeExists(path.toString)
  }
}

