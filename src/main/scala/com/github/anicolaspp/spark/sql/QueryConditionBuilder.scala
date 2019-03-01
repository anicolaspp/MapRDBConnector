package com.github.anicolaspp.spark.sql

import org.apache.spark.internal.Logging
import org.apache.spark.sql.sources._
import org.ojai.store.{Connection, QueryCondition}

object QueryConditionBuilder extends Logging {

  import collection.JavaConversions._

  def buildQueryConditionFrom(filters: List[Filter])(implicit connection: Connection): QueryCondition = createFilterCondition(filters)

  private def createFilterCondition(filters: List[Filter])(implicit connection: Connection): QueryCondition = {
    log.info("SUPPORTED FILTERS: " + filters)

    val andCondition = connection.newCondition().and()

    val finalCondition = filters
      .foldLeft(andCondition) { (partialCondition, filter) => partialCondition.condition(evalFilter(filter)) }
      .close()
      .build()

    log.info("FINAL CONDITION: " + finalCondition.toString)

    finalCondition
  }

  private def evalFilter(filter: Filter)(implicit connection: Connection): QueryCondition = {

    log.info("evalFilter: " + filter.toString)

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

    log.info("evalSingleFilter: " + filter.toString)

    val simpleCondition = filter match {
      case IsNull(field)                  => connection.newCondition().notExists(field)
      case IsNotNull(field)               => connection.newCondition().exists(field)
      case In(field, values)              => connection.newCondition().in(field, values.toList)
      case StringStartsWith(field, value) => connection.newCondition().like(field, value)
      case eq@EqualTo(_, _)               => evalEqualTo(eq)
      case gt@GreaterThan(_, _)           => evalGreaterThan(gt)
    }

    log.info("evalSingleFilter: " + filter.toString + "===============" + simpleCondition.toString)

    simpleCondition.build()
  }


  private def evalEqualTo(filter: EqualTo)(implicit connection: Connection) = filter match {
    case EqualTo(field, value: Double) => connection.newCondition().is(field, QueryCondition.Op.EQUAL, value)
    case EqualTo(field, value: Float) => connection.newCondition().is(field, QueryCondition.Op.EQUAL, value)
    case EqualTo(field, value: Int) => connection.newCondition.is(field, QueryCondition.Op.EQUAL, value)
    case EqualTo(field, value: Long) => connection.newCondition.is(field, QueryCondition.Op.EQUAL, value)
    case EqualTo(field, value: Short) => connection.newCondition.is(field, QueryCondition.Op.EQUAL, value)
    case EqualTo(field, value: String) => connection.newCondition().is(field, QueryCondition.Op.EQUAL, value)

    case EqualTo(_, _) => connection.newCondition()
  }

  private def evalLessThan(filter: LessThan)(implicit connection: Connection) = filter match {
    case LessThan(field, value: Double) => connection.newCondition().is(field, QueryCondition.Op.LESS, value)
    case LessThan(field, value: Float) => connection.newCondition().is(field, QueryCondition.Op.LESS, value)
    case LessThan(field, value: Int) => connection.newCondition().is(field, QueryCondition.Op.LESS, value)
    case LessThan(field, value: Long) => connection.newCondition().is(field, QueryCondition.Op.LESS, value)
    case LessThan(field, value: Short) => connection.newCondition().is(field, QueryCondition.Op.LESS, value)
    case LessThan(field, value: String) => connection.newCondition().is(field, QueryCondition.Op.LESS, value)

    case LessThan(_, _) => connection.newCondition()
  }

  private def evalLessThanEqual(filter: LessThanOrEqual)(implicit connection: Connection) = filter match {
    case LessThanOrEqual(field, value: Double) => connection.newCondition().is(field, QueryCondition.Op.LESS_OR_EQUAL, value)
    case LessThanOrEqual(field, value: Float) => connection.newCondition().is(field, QueryCondition.Op.LESS_OR_EQUAL, value)
    case LessThanOrEqual(field, value: Int) => connection.newCondition().is(field, QueryCondition.Op.LESS_OR_EQUAL, value)
    case LessThanOrEqual(field, value: Long) => connection.newCondition().is(field, QueryCondition.Op.LESS_OR_EQUAL, value)
    case LessThanOrEqual(field, value: Short) => connection.newCondition().is(field, QueryCondition.Op.LESS_OR_EQUAL, value)
    case LessThanOrEqual(field, value: String) => connection.newCondition().is(field, QueryCondition.Op.LESS_OR_EQUAL, value)

    case LessThanOrEqual(_, _) => connection.newCondition()
  }

  private def evalGreaterThan(filter: GreaterThan)(implicit connection: Connection) = filter match {
    case GreaterThan(field, value: Double) => connection.newCondition.is(field, QueryCondition.Op.GREATER, value)
    case GreaterThan(field, value: Float) => connection.newCondition.is(field, QueryCondition.Op.GREATER, value)
    case GreaterThan(field, value: Int) => connection.newCondition.is(field, QueryCondition.Op.GREATER, value)
    case GreaterThan(field, value: Long) => connection.newCondition.is(field, QueryCondition.Op.GREATER, value)
    case GreaterThan(field, value: Short) => connection.newCondition.is(field, QueryCondition.Op.GREATER, value)
    case GreaterThan(field, value: String) => connection.newCondition.is(field, QueryCondition.Op.GREATER, value)

    case GreaterThan(_, _) => connection.newCondition()
  }

  private def evalGreaterThanEqual(filter: GreaterThan)(implicit connection: Connection) = filter match {
    case GreaterThan(field, value: Double) => connection.newCondition.is(field, QueryCondition.Op.GREATER_OR_EQUAL, value)
    case GreaterThan(field, value: Float) => connection.newCondition.is(field, QueryCondition.Op.GREATER_OR_EQUAL, value)
    case GreaterThan(field, value: Int) => connection.newCondition.is(field, QueryCondition.Op.GREATER_OR_EQUAL, value)
    case GreaterThan(field, value: Long) => connection.newCondition.is(field, QueryCondition.Op.GREATER_OR_EQUAL, value)
    case GreaterThan(field, value: Short) => connection.newCondition.is(field, QueryCondition.Op.GREATER_OR_EQUAL, value)
    case GreaterThan(field, value: String) => connection.newCondition.is(field, QueryCondition.Op.GREATER_OR_EQUAL, value)

    case GreaterThan(_, _) => connection.newCondition()
  }
}
