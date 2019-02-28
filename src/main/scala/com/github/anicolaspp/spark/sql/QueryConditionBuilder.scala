package com.github.anicolaspp.spark.sql

import org.apache.spark.internal.Logging
import org.apache.spark.sql.sources._
import org.ojai.store.{Connection, QueryCondition}

object QueryConditionBuilder extends Logging {

  def condition(filters: List[Filter])(implicit connection: Connection): QueryCondition = createFilterCondition(filters)

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
      case IsNotNull(field) => connection.newCondition().exists(field)

      case eq @ EqualTo(_, _) => evalEqualTo(eq)
      case gt @ GreaterThan(_, _) => evalGreaterThan(gt)
    }

    log.info("evalSingleFilter: " + filter.toString + "===============" + simpleCondition.toString)

    simpleCondition.build()
  }

  private def evalEqualTo(filter: EqualTo)(implicit connection: Connection) = filter match {
    case EqualTo(field, value: String) => connection.newCondition().is(field, QueryCondition.Op.EQUAL, value)
    case EqualTo(field, value: Int) => connection.newCondition.is(field, QueryCondition.Op.EQUAL, value)
  }

  private def evalGreaterThan(filter: GreaterThan)(implicit connection: Connection) = filter match {
    case GreaterThan(field, value: String) => connection.newCondition.is(field, QueryCondition.Op.GREATER, value)
    case GreaterThan(field, value: Int) => connection.newCondition.is(field, QueryCondition.Op.GREATER, value)
  }

  private def createFilterCondition(filters: List[Filter])(implicit connection: Connection): QueryCondition = {

    log.info("SUPPORTED FILTERS: " + filters)

    val finalCondition = filters.foldLeft(connection.newCondition().and()) { (condition, filter) =>

      condition.condition(evalFilter(filter))

    }
      .close()
      .build()

    log.info("FINAL CONDITION: " + finalCondition.toString)

    finalCondition
  }

}
