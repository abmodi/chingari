package com.abmodi.chingari.physical

import com.abmodi.chingari.core._

trait Expression {
  def eval(input : ColumnBatch): ColumnVector
}

case class LiteralIntExpression(value : Int) extends Expression {
  override def eval(input: ColumnBatch): ColumnVector = LiteralVector(value, IntegerType, input.numRows)
}

case class LiteralBoolExpression(value : Boolean) extends Expression {
  override def eval(input: ColumnBatch): ColumnVector = LiteralVector(value, BooleanType, input.numRows)
}

case class LiteralLongExpression(value : Long) extends Expression {
  override def eval(input: ColumnBatch): ColumnVector = LiteralVector(value, LongType, input.numRows)
}

case class LiteralDoubleExpression(value : Double) extends Expression {
  override def eval(input: ColumnBatch): ColumnVector = LiteralVector(value, DoubleType, input.numRows)
}

case class LiteralStringExpression(value : String) extends Expression {
  override def eval(input: ColumnBatch): ColumnVector = LiteralVector(value, StringType, input.numRows)
}

