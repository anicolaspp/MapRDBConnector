package com.github.anicolaspp.spark.sql

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.ojai.types.{ODate, OTime, OTimestamp}
import org.ojai.{Document, Value}
import com.mapr.db.rowcol.DBList

object ParsableDocument {

  import collection.JavaConverters._

  implicit class ParsableDocument(document: Document) {
    def get(field: StructField) = getField(document, field)

    private def getField(doc: Document, field: StructField): Any = (doc.getValue(field.name), doc.getValue(field.name).getType) match {
      case (value, Value.Type.ARRAY) => createArray(value.getList.toArray)
      case (value, Value.Type.BINARY) => value.getBinary
      case (value, Value.Type.BOOLEAN) => value.getBoolean
      case (value, Value.Type.BYTE) => value.getByte
      case (value, Value.Type.DATE) => value.getDate.toDate
      case (value, Value.Type.DECIMAL) => value.getDecimal
      case (value, Value.Type.DOUBLE) => value.getDouble
      case (value, Value.Type.FLOAT) => value.getFloat
      case (value, Value.Type.INT) => value.getInt
      case (value, Value.Type.INTERVAL) => null //TODO: Find the actual type that corresponds to this
      case (value, Value.Type.LONG) => value.getLong
      case (value, Value.Type.MAP) => createMap(value.getMap)
      case (value, Value.Type.NULL) => null
      case (value, Value.Type.SHORT) => value.getShort
      case (value, Value.Type.STRING) => value.getString
      case (value, Value.Type.TIME) => new java.sql.Timestamp(value.getTime.getMilliSecond)
      case (value, Value.Type.TIMESTAMP) => new java.sql.Timestamp(value.getTimestamp.getMilliSecond)
    }

    private def getValue(value: Any): Any = {
      value match {
        case v: OTimestamp => new java.sql.Timestamp(v.getMilliSecond)
        case v: OTime => new java.sql.Timestamp(v.getMilliSecond)
        case v: ODate => v.toDate
        case v: java.util.Map[String, Object] => createMap(v)
        case v: DBList => createArray(v.toArray())
        case v: Any => v
      }
    }

    private def createMap(map: java.util.Map[String, Object]): Row = {
      val scalaMap = map.asScala

      val values = scalaMap
        .keySet
        .foldLeft(List.empty[Any])((xs, field) => getValue(scalaMap(field)) :: xs)
        .reverse

      Row.fromSeq(values)
    }

    private def createArray(array: Array[Object]): Array[Any] = {
      array.map(getValue)
    }
  }


}
