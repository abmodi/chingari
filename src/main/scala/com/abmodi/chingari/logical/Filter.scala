package com.abmodi.chingari.logical

import com.abmodi.chingari.core.Schema

case class Filter(input: LogicalPlan,
                  filter: LogicalExpression) extends LogicalPlan {
  override def output: Schema = input.output

  override def children: Seq[LogicalPlan] = Seq(input)

  override def toString: String = s"Filter: $filter"
}
