package com.abmodi.chingari.physical

case class CountExpression(expr : Expression) extends AggregateExpression {
  override def inputExpression(): Expression = expr

  override def createAccumulator(): Accumulator = new Accumulator {
    var res : Long = 0
    override def accumulate(value: Any): Unit = res += 1

    override def finalValue(): Any = res
  }
}