package com.abmodi.chingari.core

import com.abmodi.chingari.logical.{Aggregate, Filter, LogicalExpression, LogicalPlan, Project}
import com.abmodi.chingari.logical.AggregateExpression
import com.abmodi.chingari.optimizer.{Optimizer, Strategy}
import com.abmodi.chingari.physical.PhysicalPlan

case class DataFrame(plan : LogicalPlan) {
  private lazy val _optimizedPlan = Optimizer().optimize(plan)
  private lazy val _physicalPlan = Strategy(_optimizedPlan)

  def project(projectList : Seq[LogicalExpression]): DataFrame = {
    DataFrame(Project(plan, projectList))
  }

  def filter(filter : LogicalExpression) : DataFrame = {
    DataFrame(Filter(plan, filter))
  }

  def aggregate(groupExpressions : Seq[LogicalExpression], aggExpressions : Seq[AggregateExpression]) : DataFrame = {
    DataFrame(Aggregate(plan, aggExpressions, groupExpressions))
  }

  def logicalPlan(): LogicalPlan = {
    println(plan.pretty)
    plan
  }

  def optimizedPlan(): LogicalPlan = {
    println(_optimizedPlan.pretty)
    _optimizedPlan
  }

  def physicalPlan() : PhysicalPlan = {
    println(_physicalPlan.pretty())
    _physicalPlan
  }

  def execute() : Iterator[ColumnBatch] = {
    _physicalPlan.execute()
  }
}
