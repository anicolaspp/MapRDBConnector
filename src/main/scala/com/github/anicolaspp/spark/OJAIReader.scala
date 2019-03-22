package com.github.anicolaspp.spark

import com.github.anicolaspp.spark.OJAIReader.Cell
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.types.{DataType, StructType}

@DeveloperApi
trait OJAIReader {
  def readFrom(partition: Iterator[Cell],
               table: String,
               schema: StructType,
               right: String): Iterator[String]
}

object OJAIReader {

  @DeveloperApi
  def defaultPartitionReader: OJAIReader = PartitionQueryRunner

  /**
    * Used to project the exact column we need to filter the MapR-DB table. We can use Cell instead of passing the
    * entire Row to reduce the memory footprint.
    *
    * @param value    Spark value of the Row at the specific column.
    * @param dataType The corresponding data type
    */
  case class Cell(value: Any, dataType: DataType)

}