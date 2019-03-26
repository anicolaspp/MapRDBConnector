package com.github.anicolaspp.ojai

import com.github.anicolaspp.ojai.OJAISparkPartitionReader.Cell
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.types.{DataType, StructType}

@DeveloperApi
trait OJAISparkPartitionReader {
  def readFrom(partition: Iterator[Cell],
               table: String,
               schema: StructType,
               right: String): Iterator[String]
}

object OJAISparkPartitionReader {

  @DeveloperApi
  def groupedPartitionReader(batchSize: Int = 20): OJAISparkPartitionReader = new GroupedPartitionQueryRunner(batchSize)

  @DeveloperApi
  def sequentialPartitionReader: OJAISparkPartitionReader = new GroupedPartitionQueryRunner(1)

  /**
    * Used to project the exact column we need to filter the MapR-DB table. We can use Cell instead of passing the
    * entire Row to reduce the memory footprint.
    *
    * @param value    Spark value of the Row at the specific column.
    * @param dataType The corresponding data type
    */
  private[anicolaspp] case class Cell(value: Any, dataType: DataType)

}