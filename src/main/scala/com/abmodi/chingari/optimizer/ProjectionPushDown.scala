package com.abmodi.chingari.optimizer
import com.abmodi.chingari.logical.{Aggregate, AggregateExpression, BinaryExpression, Column, ColumnIndex, Filter, LogicalExpression, LogicalPlan, Project, Scan}

import scala.collection.mutable

object ProjectionPushDown extends OptimizerRule {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    pushdown(plan, mutable.Set())
  }

  private def pushdown(plan : LogicalPlan, columnNames : mutable.Set[String]) : LogicalPlan = {
    plan match {
      case Project(input, projectList) =>
        projectList.foreach(extractColumns(_, input, columnNames))
        val inp = pushdown(input, columnNames)
        Project(inp, projectList)
      case Filter(input, filter) =>
        extractColumns(filter, input, columnNames)
        val inp = pushdown(input, columnNames)
        Filter(inp, filter)
      case Aggregate(input, aggExpressions, groupingExpression) =>
        aggExpressions.foreach(extractColumns(_, input, columnNames))
        groupingExpression.foreach(extractColumns(_, input, columnNames))
        Aggregate(pushdown(input, columnNames), aggExpressions, groupingExpression)
      case Scan(dataSource, _) =>
        val projectList = dataSource.schema.fields.map(_.name).filter(columnNames.contains)
        Scan(dataSource, projectList)
    }
  }

  private def extractColumns(expr : LogicalExpression, input : LogicalPlan, columns: mutable.Set[String]): Unit = {
    expr match {
      case Column(name) => columns.add(name)
      case ColumnIndex(index) => columns += input.output.fields(index).name
      case b : BinaryExpression => {
        extractColumns(b.leftChild(), input, columns)
        extractColumns(b.rightChild(), input, columns)
      }
      case a : AggregateExpression => {
        extractColumns(a.child(), input, columns)
      }
      case _ => ()
    }
  }

}
