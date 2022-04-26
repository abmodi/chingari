package com.abmodi.chingari.physical

import com.abmodi.chingari.core.{ColumnBatch, GenericColumnVector, Schema}

case class FilterExec(input: PhysicalPlan,
                      filter: Expression) extends PhysicalPlan {
  override def output(): Schema = input.output()

  override def children: Seq[PhysicalPlan] = Seq(input)

  override def execute(): Seq[ColumnBatch] = {
    input.execute().map(batch => {
      val selectionVector = filter.eval(batch)
      val filteredColumnVectors = batch.columnVectors.map(cv => {
        val filteredArray = for {
          index <- (0 until selectionVector.size)
          if (selectionVector.getValue(index).asInstanceOf[Boolean])
        } yield cv.getValue(index)
        new GenericColumnVector(cv.dataType, filteredArray.toArray)
      })
      new ColumnBatch(batch.schema, filteredColumnVectors)
    })
  }

  override def toString: String = s"FilterExec: $filter"
}
