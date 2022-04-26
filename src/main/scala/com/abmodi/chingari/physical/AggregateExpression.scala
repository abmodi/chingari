package com.abmodi.chingari.physical

import com.abmodi.chingari.core.{ColumnBatch, ColumnVector}

trait AggregateExpression extends Expression {
  def inputExpression(): Expression

  def createAccumulator(): Accumulator

  override def eval(input: ColumnBatch): ColumnVector = throw new UnsupportedOperationException
}

trait Accumulator {
  def accumulate(value : Any) : Unit
  def finalValue() : Any
}
