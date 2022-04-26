package com.abmodi.chingari.logical

import com.abmodi.chingari.core.Schema

case class Aggregate(input: LogicalPlan,
                     aggExpressions: Seq[AggregateExpression],
                     groupingExpression: Seq[LogicalExpression]) extends LogicalPlan {
  override def output: Schema = Schema(aggExpressions.map(_.output(input))
    ++ groupingExpression.map(_.output(input)))

  override def children: Seq[LogicalPlan] = Seq(input)

  override def toString: String = {
    s"Aggregate: group expr = ${groupingExpression.map(_.toString).mkString(",")}" +
      s"agg expr = ${aggExpressions.map(_.toString).mkString(",")}"
  }
}
