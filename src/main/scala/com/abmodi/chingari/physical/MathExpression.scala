package com.abmodi.chingari.physical

import com.abmodi.chingari.core.{ColumnVector, DataType, DoubleType, GenericColumnVector, IntegerType, LongType, StringType}

abstract class MathExpression(left : Expression,
                                   right : Expression,
                                  ) extends BinaryExpression(left, right) {
  override def evaluate(l: ColumnVector, r: ColumnVector): ColumnVector = {
    val arr = Array.ofDim[Any](l.size)
    for (index <- 0 until l.size) {
      val value = evaluate(l.getValue(index), r.getValue(index), l.dataType)
      arr(index) = value
    }
    new GenericColumnVector(l.dataType, arr)
  }

  def evaluate(l : Any, r : Any, dataType: DataType) : Any
}

case class AddExpression(l : Expression,
                         r : Expression) extends MathExpression(l, r) {
  override def evaluate(l: Any, r: Any, dataType: DataType): Any = {
    dataType match {
      case IntegerType => l.asInstanceOf[Int] + r.asInstanceOf[Int]
      case LongType => l.asInstanceOf[Long] + r.asInstanceOf[Long]
      case DoubleType => l.asInstanceOf[Double] + r.asInstanceOf[Double]
      case StringType => l.asInstanceOf[String] + r.asInstanceOf[String]
      case _ => throw new IllegalStateException("Unsupported data type in addition")
    }
  }

  override def toString: String = s"$l + $r"
}

case class SubtractExpression(l : Expression,
                         r : Expression) extends MathExpression(l, r) {
  override def evaluate(l: Any, r: Any, dataType: DataType): Any = {
    dataType match {
      case IntegerType => l.asInstanceOf[Int] - r.asInstanceOf[Int]
      case LongType => l.asInstanceOf[Long] - r.asInstanceOf[Long]
      case DoubleType => l.asInstanceOf[Double] - r.asInstanceOf[Double]
      case _ => throw new IllegalStateException("Unsupported data type in subtraction")
    }
  }

  override def toString: String = s"$l - $r"
}

case class MultiplyExpression(l : Expression,
                         r : Expression) extends MathExpression(l, r) {
  override def evaluate(l: Any, r: Any, dataType: DataType): Any = {
    dataType match {
      case IntegerType => l.asInstanceOf[Int] * r.asInstanceOf[Int]
      case LongType => l.asInstanceOf[Long] * r.asInstanceOf[Long]
      case DoubleType => l.asInstanceOf[Double] * r.asInstanceOf[Double]
      case _ => throw new IllegalStateException("Unsupported data type in multiplication")
    }
  }

  override def toString: String = s"$l * $r"
}

case class DivisionExpression(l : Expression,
                         r : Expression) extends MathExpression(l, r) {
  override def evaluate(l: Any, r: Any, dataType: DataType): Any = {
    dataType match {
      case IntegerType => l.asInstanceOf[Int] / r.asInstanceOf[Int]
      case LongType => l.asInstanceOf[Long] / r.asInstanceOf[Long]
      case DoubleType => l.asInstanceOf[Double] / r.asInstanceOf[Double]
      case _ => throw new IllegalStateException("Unsupported data type in division")
    }
  }

  override def toString: String = s"$l / $r"
}