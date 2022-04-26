package com.abmodi.chingari.physical

import com.abmodi.chingari.core.{ColumnBatch, ColumnVector}

abstract class BinaryExpression(l : Expression,
                                r : Expression) extends Expression {
  override def eval(input: ColumnBatch): ColumnVector = {
    val ll = l.eval(input)
    val rr = r.eval(input)

    assert(ll.size == rr.size)
    if (ll.dataType != rr.dataType) {
      throw new IllegalStateException(
        "Binary expression operands do not have the same type: " +
          "${ll.getType()} != ${rr.getType()}")
    }
    evaluate(ll, rr)
  }

  def evaluate(l: ColumnVector, r: ColumnVector): ColumnVector
}
