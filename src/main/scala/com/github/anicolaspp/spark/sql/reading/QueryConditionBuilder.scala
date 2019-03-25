package com.github.anicolaspp.spark.sql.reading

import java.sql.Timestamp

import org.apache.spark.internal.Logging
import org.apache.spark.sql.sources._
import org.ojai.store.{Connection, QueryCondition}
import org.ojai.types.OTimestamp

object QueryConditionBuilder extends Logging {

  import collection.JavaConversions._

  def buildQueryConditionFrom(filters: List[Filter])(implicit connection: Connection): String =
    createFilterCondition(filters).asJsonString()

  def addTabletInfo(queryJson: String, queryCondition: String): String =
    if (queryJson == "{}") {
      queryCondition
    } else {
      "{\"$and\":[" + queryJson + "," + queryCondition + "]}"
    }

  /**
    * Spark sends individual filters down that we need to concat using AND. This function evaluates each filter
    * recursively and creates the corresponding OJAI query.
    *
    * @param filters
    * @param connection
    * @return
    */
  private def createFilterCondition(filters: List[Filter])(implicit connection: Connection): QueryCondition = {
    log.debug(s"FILTERS TO PUSH DOWN: $filters")

    val andCondition = connection.newCondition().and()

    val finalCondition = filters
      .foldLeft(andCondition) { (partialCondition, filter) => partialCondition.condition(evalFilter(filter)) }
      .close()
      .build()

    log.debug(s"FINAL OJAI QUERY CONDITION: ${finalCondition.toString}")

    finalCondition
  }

  /**
    * Translate a Spark Filter to an OJAI query.
    *
    * It recursively translate nested filters.
    *
    * @param filter
    * @param connection
    * @return
    */
  private def evalFilter(filter: Filter)(implicit connection: Connection): QueryCondition = {

    log.debug("evalFilter: " + filter.toString)

    val condition = filter match {

      case Or(left, right) => connection.newCondition()
        .or()
        .condition(evalFilter(left))
        .condition(evalFilter(right))
        .close()
        .build()

      case And(left, right) => connection.newCondition()
        .and()
        .condition(evalFilter(left))
        .condition(evalFilter(right))
        .close()
        .and()

      case singleFilter => evalSingleFilter(singleFilter)
    }

    condition
  }

  private def evalSingleFilter(filter: Filter)(implicit connection: Connection) = {

    val simpleCondition = filter match {
      case IsNull(field) => connection.newCondition().notExists(field)
      case IsNotNull(field) => connection.newCondition().exists(field)
      case In(field, values) => connection.newCondition().in(field, values.toList)
      case StringStartsWith(field, value) => connection.newCondition().matches(field, value)
      case eq@EqualTo(_, _) => evalEqualTo(eq)
      case lt@LessThan(_, _) => evalLessThan(lt)
      case le@LessThanOrEqual(_, _) => evalLessThanEqual(le)
      case gt@GreaterThan(_, _) => evalGreaterThan(gt)
      case ge@GreaterThanOrEqual(_, _) => evalGreaterThanEqual(ge)
    }

    log.debug("evalSingleFilter: " + filter.toString + " =============== " + simpleCondition.toString)

    simpleCondition.build()
  }


  private def evalEqualTo(filter: EqualTo)(implicit connection: Connection) = filter match {
    case EqualTo(field, value: Double) => connection.newCondition().is(field, QueryCondition.Op.EQUAL, value)
    case EqualTo(field, value: Float) => connection.newCondition().is(field, QueryCondition.Op.EQUAL, value)
    case EqualTo(field, value: Int) => connection.newCondition.is(field, QueryCondition.Op.EQUAL, value)
    case EqualTo(field, value: Long) => connection.newCondition.is(field, QueryCondition.Op.EQUAL, value)
    case EqualTo(field, value: Short) => connection.newCondition.is(field, QueryCondition.Op.EQUAL, value)
    case EqualTo(field, value: Boolean) => connection.newCondition.is(field, QueryCondition.Op.EQUAL, value)
    case EqualTo(field, value: Byte) => connection.newCondition.is(field, QueryCondition.Op.EQUAL, value)
    case EqualTo(field, value: String) => connection.newCondition().is(field, QueryCondition.Op.EQUAL, value)
    case EqualTo(field, value: Timestamp) => connection.newCondition.is(field, QueryCondition.Op.EQUAL,new OTimestamp(value.getTime))

    case EqualTo(_, _) => universalCondition
  }

  private def evalLessThan(filter: LessThan)(implicit connection: Connection) = filter match {
    case LessThan(field, value: Double) => connection.newCondition().is(field, QueryCondition.Op.LESS, value)
    case LessThan(field, value: Float) => connection.newCondition().is(field, QueryCondition.Op.LESS, value)
    case LessThan(field, value: Int) => connection.newCondition().is(field, QueryCondition.Op.LESS, value)
    case LessThan(field, value: Long) => connection.newCondition().is(field, QueryCondition.Op.LESS, value)
    case LessThan(field, value: Short) => connection.newCondition().is(field, QueryCondition.Op.LESS, value)
    case LessThan(field, value: Boolean) => connection.newCondition().is(field, QueryCondition.Op.LESS, value)
    case LessThan(field, value: Byte) => connection.newCondition().is(field, QueryCondition.Op.LESS, value)
    case LessThan(field, value: String) => connection.newCondition().is(field, QueryCondition.Op.LESS, value)
    case LessThan(field, value: Timestamp) => connection.newCondition.is(field, QueryCondition.Op.LESS, new OTimestamp(value.getTime))

    case LessThan(_, _) => universalCondition
  }

  private def evalLessThanEqual(filter: LessThanOrEqual)(implicit connection: Connection) = filter match {
    case LessThanOrEqual(field, value: Double) => connection.newCondition().is(field, QueryCondition.Op.LESS_OR_EQUAL, value)
    case LessThanOrEqual(field, value: Float) => connection.newCondition().is(field, QueryCondition.Op.LESS_OR_EQUAL, value)
    case LessThanOrEqual(field, value: Int) => connection.newCondition().is(field, QueryCondition.Op.LESS_OR_EQUAL, value)
    case LessThanOrEqual(field, value: Long) => connection.newCondition().is(field, QueryCondition.Op.LESS_OR_EQUAL, value)
    case LessThanOrEqual(field, value: Short) => connection.newCondition().is(field, QueryCondition.Op.LESS_OR_EQUAL, value)
    case LessThanOrEqual(field, value: Boolean) => connection.newCondition().is(field, QueryCondition.Op.LESS_OR_EQUAL, value)
    case LessThanOrEqual(field, value: Byte) => connection.newCondition().is(field, QueryCondition.Op.LESS_OR_EQUAL, value)
    case LessThanOrEqual(field, value: String) => connection.newCondition().is(field, QueryCondition.Op.LESS_OR_EQUAL, value)
    case LessThanOrEqual(field, value: Timestamp) => connection.newCondition.is(field, QueryCondition.Op.LESS_OR_EQUAL, new OTimestamp(value.getTime))

    case LessThanOrEqual(_, _) => universalCondition
  }

  private def evalGreaterThan(filter: GreaterThan)(implicit connection: Connection) = filter match {
    case GreaterThan(field, value: Double) => connection.newCondition.is(field, QueryCondition.Op.GREATER, value)
    case GreaterThan(field, value: Float) => connection.newCondition.is(field, QueryCondition.Op.GREATER, value)
    case GreaterThan(field, value: Int) => connection.newCondition.is(field, QueryCondition.Op.GREATER, value)
    case GreaterThan(field, value: Long) => connection.newCondition.is(field, QueryCondition.Op.GREATER, value)
    case GreaterThan(field, value: Short) => connection.newCondition.is(field, QueryCondition.Op.GREATER, value)
    case GreaterThan(field, value: Boolean) => connection.newCondition.is(field, QueryCondition.Op.GREATER, value)
    case GreaterThan(field, value: Byte) => connection.newCondition.is(field, QueryCondition.Op.GREATER, value)
    case GreaterThan(field, value: String) => connection.newCondition.is(field, QueryCondition.Op.GREATER, value)
    case GreaterThan(field, value: Timestamp) => connection.newCondition.is(field, QueryCondition.Op.GREATER, new OTimestamp(value.getTime))

    case GreaterThan(_, _) => universalCondition
  }

  private def evalGreaterThanEqual(filter: GreaterThanOrEqual)(implicit connection: Connection) = filter match {
    case GreaterThanOrEqual(field, value: Double) => connection.newCondition.is(field, QueryCondition.Op.GREATER_OR_EQUAL, value)
    case GreaterThanOrEqual(field, value: Float) => connection.newCondition.is(field, QueryCondition.Op.GREATER_OR_EQUAL, value)
    case GreaterThanOrEqual(field, value: Int) => connection.newCondition.is(field, QueryCondition.Op.GREATER_OR_EQUAL, value)
    case GreaterThanOrEqual(field, value: Long) => connection.newCondition.is(field, QueryCondition.Op.GREATER_OR_EQUAL, value)
    case GreaterThanOrEqual(field, value: Short) => connection.newCondition.is(field, QueryCondition.Op.GREATER_OR_EQUAL, value)
    case GreaterThanOrEqual(field, value: Boolean) => connection.newCondition.is(field, QueryCondition.Op.GREATER_OR_EQUAL, value)
    case GreaterThanOrEqual(field, value: Byte) => connection.newCondition.is(field, QueryCondition.Op.GREATER_OR_EQUAL, value)
    case GreaterThanOrEqual(field, value: String) => connection.newCondition.is(field, QueryCondition.Op.GREATER_OR_EQUAL, value)
    case GreaterThanOrEqual(field, value: Timestamp) => connection.newCondition.is(field, QueryCondition.Op.GREATER_OR_EQUAL, new OTimestamp(value.getTime))

    case GreaterThanOrEqual(_, _) => universalCondition
  }

  /**
    * If we don't know how to parse an specific type, this condition matches all records
    *
    * @param connection
    * @return
    */
  private def universalCondition(implicit connection: Connection) = connection.newCondition().exists("_id")
}