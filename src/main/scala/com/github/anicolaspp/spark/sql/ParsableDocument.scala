package com.github.anicolaspp.spark.sql

import org.apache.spark.sql.types._
import org.ojai.{Document, Value}

object ParsableDocument {

  implicit class ParsableDocument(document: Document) {
    def get(field: StructField): Any = getAnyFromDocument(document, field.name, field.dataType)

    private def getAnyFromDocument(doc: Document, name: String, dataType: DataType) = dataType match {
      case BinaryType => doc.getBinary(name)
      case BooleanType => doc.getBooleanObj(name)
      case ByteType => doc.getByteObj(name)
      case DateType => doc.getDate(name)
      case _: DecimalType => doc.getDecimal(name)
      case DoubleType => doc.getDoubleObj(name)
      case FloatType => doc.getFloatObj(name)
      case IntegerType => doc.getIntObj(name)
      case ArrayType(_, _) => doc.getList(name) //TODO Need to check what happens in the regular connector
      case LongType => doc.getLongObj(name)
      case StructType(_) => doc.getMap(name) //TODO This might need to be done recursively based on the values in the StructType
      case ShortType => doc.getShortObj(name)
      case StringType => doc.getString(name)
      case TimestampType => {
        val valObj = doc.getValue(name)
        if (valObj.getType == Value.TYPE_CODE_TIME) {
          new java.sql.Timestamp(valObj.getTime.getMilliSecond)
        } else {
          new java.sql.Timestamp(valObj.getTimestamp.getMilliSecond)
        }
      }
      //TODO this will probably not automatically convert from OInterval to CalendarIntervalType
      // It seems that this will need to be convert to a correct string representation
      case CalendarIntervalType => doc.getInterval(name)
      case _ => throw new IllegalArgumentException(s"Unknown type $dataType for field $name")
    }
  }
}
