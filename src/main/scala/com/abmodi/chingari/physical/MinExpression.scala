package com.abmodi.chingari.physical

case class MinExpression(expr : Expression) extends AggregateExpression {
  override def inputExpression(): Expression = expr

  override def createAccumulator(): Accumulator = MinAccumulator()
}

case class MinAccumulator() extends Accumulator {

  var value: Any = null

  override def accumulate(value: Any): Unit = {
    if (this.value == null) {
      this.value = value
    } else {
      this.value = value match {
        case v : Int => if (this.value.asInstanceOf[Int] < v) {
          this.value
        } else {
          v
        }
        case v : Long => if (this.value.asInstanceOf[Long] < v) {
          this.value
        } else {
          v
        }
        case v : Double => if (this.value.asInstanceOf[Double] < v) {
          this.value
        } else {
          v
        }
        case _ => throw new IllegalStateException("Aggregation not supported")
      }
    }
  }

  override def finalValue(): Any = value
}