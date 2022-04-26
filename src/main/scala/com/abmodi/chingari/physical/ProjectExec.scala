package com.abmodi.chingari.physical

import com.abmodi.chingari.core.{ColumnBatch, Schema}

case class ProjectExec(input: PhysicalPlan,
                       schema: Schema,
                       expressions: Seq[Expression]) extends PhysicalPlan {
  override def output(): Schema = schema

  override def children: Seq[PhysicalPlan] = Seq(input)

  override def execute(): Iterator[ColumnBatch] = {
    input.execute().map({
      batch => {
        val columns = expressions.map(expr => expr.eval(batch))
        new ColumnBatch(schema, columns)
      }
    })
  }

  override def toString: String = s"ProjectExec: ${expressions.map(_.toString).mkString(",")}"
}
