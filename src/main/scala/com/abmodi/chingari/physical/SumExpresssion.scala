package com.abmodi.chingari.physical

case class SumExpression(expr : Expression) extends AggregateExpression {
  override def inputExpression(): Expression = expr

  override def createAccumulator(): Accumulator = SumAccumulator()
}

case class SumAccumulator() extends Accumulator {

  var value: Any = null

  override def accumulate(value: Any): Unit = {
    if (this.value == null) {
      this.value = value
    } else {
      this.value = value match {
        case v : Int => {
          val ev = this.value.asInstanceOf[Int]
          ev + v
        }
        case v : Long => {
          val ev = this.value.asInstanceOf[Long]
          ev + v
        }
        case v : Double => {
          val ev = this.value.asInstanceOf[Double]
          ev + v
        }
        case _ => throw new IllegalStateException("Aggregation not supported")
      }
    }
  }

  override def finalValue(): Any = value
}