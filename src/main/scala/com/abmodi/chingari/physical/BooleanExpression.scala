package com.abmodi.chingari.physical

import com.abmodi.chingari.core._

abstract class BooleanExpression(l : Expression,
                                 r: Expression) extends BinaryExpression(l, r) {
  override def evaluate(l: ColumnVector, r: ColumnVector): ColumnVector = {
    val arr = Array.ofDim[Any](l.size)

    for(index <- (0 until l.size)) {
      arr(index) = compare(l.getValue(index), r.getValue(index), l.dataType)
    }

    new GenericColumnVector(BooleanType, arr)
  }

  def compare(l : Any, r : Any, dt : DataType) : Boolean

  def toBool(value : Any) : Boolean = {
    value match {
      case b : Boolean => b
      case i : Int => i != 0
      case l : Long => l != 0
      case d : Double => d != 0
      case _ => false
    }
  }
}

case class AndExpression(l : Expression,
                         r : Expression) extends BooleanExpression(l, r) {
  override def compare(l : Any, r : Any, dt : DataType): Boolean = {
    toBool(l) && toBool(r)
  }

  override def toString: String = s"$l && $r"
}

case class OrExpression(l : Expression,
                        r : Expression) extends BooleanExpression(l, r) {
  override def compare(l : Any, r : Any, dt : DataType): Boolean = {
    toBool(l) || toBool(r)
  }

  override def toString: String = s"$l || $r"
}

case class EqExpression(l : Expression,
                        r : Expression) extends BooleanExpression(l, r) {
  override def compare(l : Any, r : Any, dt : DataType): Boolean = {
    dt match {
      case IntegerType => l.asInstanceOf[Int] == r.asInstanceOf[Int]
      case BooleanType => l.asInstanceOf[Boolean] == r.asInstanceOf[Boolean]
      case LongType => l.asInstanceOf[Long] == r.asInstanceOf[Long]
      case DoubleType => l.asInstanceOf[Double] == r.asInstanceOf[Double]
      case StringType => l.asInstanceOf[String] == r.asInstanceOf[String]
      case _ => throw new IllegalStateException("Equality not allowed")
    }
  }

  override def toString: String = s"$l == $r"
}

case class NeqExpression(l : Expression,
                         r : Expression) extends BooleanExpression(l, r) {
  override def compare(l : Any, r : Any, dt : DataType): Boolean = {
    dt match {
      case IntegerType => l.asInstanceOf[Int] != r.asInstanceOf[Int]
      case BooleanType => l.asInstanceOf[Boolean] != r.asInstanceOf[Boolean]
      case LongType => l.asInstanceOf[Long] != r.asInstanceOf[Long]
      case DoubleType => l.asInstanceOf[Double] != r.asInstanceOf[Double]
      case StringType => l.asInstanceOf[String] != r.asInstanceOf[String]
      case _ => throw new IllegalStateException("Not equality not allowed")
    }
  }

  override def toString: String = s"$l != $r"
}

case class GtExpression(l : Expression,
                        r: Expression) extends BooleanExpression(l, r) {
  override def compare(l : Any, r : Any, dt : DataType): Boolean = {
    dt match {
      case IntegerType => l.asInstanceOf[Int] > r.asInstanceOf[Int]
      case LongType => l.asInstanceOf[Long] > r.asInstanceOf[Long]
      case DoubleType => l.asInstanceOf[Double] > r.asInstanceOf[Double]
      case StringType => l.asInstanceOf[String] > r.asInstanceOf[String]
      case _ => throw new IllegalStateException("Comparison not allowed")
    }
  }

  override def toString: String = s"$l > $r"
}

case class GteqExpression(l : Expression,
                        r: Expression) extends BooleanExpression(l, r) {
  override def compare(l : Any, r : Any, dt : DataType): Boolean = {
    dt match {
      case IntegerType => l.asInstanceOf[Int] >= r.asInstanceOf[Int]
      case LongType => l.asInstanceOf[Long] >= r.asInstanceOf[Long]
      case DoubleType => l.asInstanceOf[Double] >= r.asInstanceOf[Double]
      case StringType => l.asInstanceOf[String] >= r.asInstanceOf[String]
      case _ => throw new IllegalStateException("Comparison not allowed")
    }
  }

  override def toString: String = s"$l >= $r"
}

case class LtExpression(l : Expression,
                        r: Expression) extends BooleanExpression(l, r) {
  override def compare(l : Any, r : Any, dt : DataType): Boolean = {
    dt match {
      case IntegerType => l.asInstanceOf[Int] < r.asInstanceOf[Int]
      case LongType => l.asInstanceOf[Long] < r.asInstanceOf[Long]
      case DoubleType => l.asInstanceOf[Double] < r.asInstanceOf[Double]
      case StringType => l.asInstanceOf[String] < r.asInstanceOf[String]
      case _ => throw new IllegalStateException("Comparison not allowed")
    }
  }

  override def toString: String = s"$l < $r"
}

case class LteqExpression(l : Expression,
                          r: Expression) extends BooleanExpression(l, r) {
  override def compare(l : Any, r : Any, dt : DataType): Boolean = {
    dt match {
      case IntegerType => l.asInstanceOf[Int] <= r.asInstanceOf[Int]
      case LongType => l.asInstanceOf[Long] <= r.asInstanceOf[Long]
      case DoubleType => l.asInstanceOf[Double] <= r.asInstanceOf[Double]
      case StringType => l.asInstanceOf[String] <= r.asInstanceOf[String]
      case _ => throw new IllegalStateException("Comparison not allowed")
    }
  }

  override def toString: String = s"$l <= $r"
}




