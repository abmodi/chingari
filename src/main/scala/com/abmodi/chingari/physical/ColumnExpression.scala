package com.abmodi.chingari.physical

import com.abmodi.chingari.core.{ColumnBatch, ColumnVector, IntegerType, LiteralVector}

case class ColumnExpression(index: Int) extends Expression {
  override def eval(input: ColumnBatch): ColumnVector = input.getColumn(index)

  override def toString: String = s"#$index"
}
