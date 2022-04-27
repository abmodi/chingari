package com.abmodi.chingari.logical

import com.abmodi.chingari.core.{BooleanType, DoubleType, IntegerType, LongType, SchemaField, StringType}

import java.util.concurrent.atomic.AtomicInteger

trait LogicalExpression {
  def output(input : LogicalPlan) : SchemaField
}

object ExpressionId {
  val id : AtomicInteger = new AtomicInteger(1)

  def getId : Int = id.getAndIncrement()
}

case class Column(name : String) extends LogicalExpression {
  override def output(input: LogicalPlan): SchemaField = {
    input.output.fields.find(field => field.name == name).getOrElse(
      throw new RuntimeException(s"Col $name not found"))
  }

  override def toString: String = s"#$name"
}

case class ColumnIndex(index : Int) extends LogicalExpression {
  override def output(input: LogicalPlan): SchemaField = input.output.fields(index)

  override def toString: String = s"#$index"
}

case object Col {
  def apply(name : String) : Column = {
    Column(name)
  }

  def apply(index : Int) : ColumnIndex = {
    ColumnIndex(index)
  }
}

case class LiteralBoolean(bool : Boolean) extends LogicalExpression {
  override def output(input: LogicalPlan): SchemaField = SchemaField(bool.toString, BooleanType)

  override def toString: String = bool.toString
}

case class LiteralInt(int : Int) extends LogicalExpression {
  override def output(input: LogicalPlan): SchemaField = SchemaField(int.toString, IntegerType)

  override def toString: String = int.toString
}

case class LiteralLong(long : Long) extends LogicalExpression {
  override def output(input: LogicalPlan): SchemaField = SchemaField(long.toString, LongType)

  override def toString: String = long.toString
}

case class LiteralDouble(double : Double) extends LogicalExpression {
  override def output(input: LogicalPlan): SchemaField = SchemaField(double.toString, DoubleType)

  override def toString: String = double.toString
}

case class LiteralString(string : String) extends LogicalExpression {
  override def output(input: LogicalPlan): SchemaField = SchemaField(string, StringType)

  override def toString: String = string
}

case object Literal {
  def apply(any : Any) : LogicalExpression = {
    any match {
      case int : Int => LiteralInt(int)
      case b : Boolean => LiteralBoolean(b)
      case l : Long => LiteralLong(l)
      case s : String => LiteralString(s)
      case d : Double => LiteralDouble(d)
    }
  }
}

abstract class BinaryExpression(name : String,
                                op : String,
                                left : LogicalExpression,
                                right : LogicalExpression) extends LogicalExpression {
  override def toString: String = s"$left $op $right"

  def leftChild(): LogicalExpression = left

  def rightChild(): LogicalExpression = right
}

abstract class BooleanBinaryExpression(name : String,
                                       op : String,
                                       left : LogicalExpression,
                                       right : LogicalExpression) extends BinaryExpression(name, op, left, right) {
  override def output(input: LogicalPlan): SchemaField = SchemaField(name, BooleanType)
}

case class And(left : LogicalExpression, right: LogicalExpression)
  extends BooleanBinaryExpression("and", "AND", left, right)

case class Or(left : LogicalExpression, right : LogicalExpression)
  extends BooleanBinaryExpression("or", "OR", left, right)

case class Eq(left : LogicalExpression, right : LogicalExpression)
  extends BooleanBinaryExpression("eq", "=", left, right)

case class Neq(left : LogicalExpression, right : LogicalExpression)
  extends BooleanBinaryExpression("neq", "!=", left, right)

case class Gt(left : LogicalExpression, right : LogicalExpression)
  extends BooleanBinaryExpression("gt", ">", left, right)

case class Gteq(left : LogicalExpression, right : LogicalExpression)
  extends BooleanBinaryExpression("gteq", ">=", left, right)

case class Lt(left : LogicalExpression, right : LogicalExpression)
  extends BooleanBinaryExpression("lt", "<", left, right)

case class Lteq(left : LogicalExpression, right : LogicalExpression)
  extends BooleanBinaryExpression("lteq", "<=", left, right)

abstract class MathExpression(name : String,
                              op : String,
                              l : LogicalExpression,
                              r : LogicalExpression) extends BinaryExpression(name, op, l, r) {
  override def output(input: LogicalPlan): SchemaField = SchemaField(name, l.output(input).datatype)
}

case class Add(l : LogicalExpression, r : LogicalExpression) extends MathExpression("add", "+", l, r)

case class Subtract(l : LogicalExpression, r : LogicalExpression) extends MathExpression("subtract", "-", l, r)

case class Multiply(l : LogicalExpression, r : LogicalExpression) extends MathExpression("multiply", "*", l, r)

case class Divide(l : LogicalExpression, r : LogicalExpression) extends MathExpression("divide", "/", l, r)

abstract class AggregateExpression(name : String,
                                   expr : LogicalExpression) extends LogicalExpression {
  override def output(input: LogicalPlan): SchemaField = SchemaField(name, expr.output(input).datatype)

  override def toString: String = s"$name($expr)"
}

case class Sum(expr : LogicalExpression) extends AggregateExpression("SUM", expr)

case class Max(expr : LogicalExpression) extends AggregateExpression("MAX", expr)

case class Min(expr : LogicalExpression) extends AggregateExpression("MIN", expr)

case class Count(expr : LogicalExpression) extends AggregateExpression("COUNT", expr) {
  override def output(input: LogicalPlan): SchemaField = SchemaField("COUNT", LongType)
}

case class Average(expr : LogicalExpression) extends AggregateExpression("AVG", expr)
