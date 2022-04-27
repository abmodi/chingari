package com.abmodi.chingari.optimizer

import com.abmodi.chingari.logical.{LogicalPlan, Scan, Project, LogicalExpression}
import com.abmodi.chingari.logical._
import com.abmodi.chingari.logical.{BinaryExpression => LBinaryExpression}
import com.abmodi.chingari.physical._
import com.abmodi.chingari.physical.{AggregateExpression => PAggregateExpression}

object Strategy {
  def apply(plan : LogicalPlan) : PhysicalPlan = {
    plan match {
      case Scan(dataSource, projectionList) => ScanExec(dataSource, projectionList)

      case p @ Project(input, projectList) => {
        val inputPlan = apply(input)
        val projectExpressions = projectList.map(createPhysicalExpr(_, input))
        ProjectExec(inputPlan, p.output, projectExpressions)
      }

      case Filter(input, filter) => {
        val filterExpression = createPhysicalExpr(filter, input)
        FilterExec(apply(input), filterExpression)
      }

      case a @ Aggregate(input, aggExpressions, groupingExpression) => {
        val gExpressions = groupingExpression.map(createPhysicalExpr(_, input))
        val aExpressions: Seq[PAggregateExpression] = aggExpressions.map(
          createPhysicalExpr(_, input).asInstanceOf[PAggregateExpression])
        HashAggregateExec(apply(input), gExpressions, aExpressions, a.output)
      }
    }
  }

  def createPhysicalExpr(expr : LogicalExpression, input : LogicalPlan) : Expression = {
    expr match {
      case LiteralLong(long) => LiteralLongExpression(long)
      case LiteralInt(int) => LiteralIntExpression(int)
      case LiteralDouble(double) => LiteralDoubleExpression(double)
      case LiteralString(string) => LiteralStringExpression(string)
      case LiteralBoolean(bool) => LiteralBoolExpression(bool)

      case Column(name) => {
        val names = input.output.fields.map(field => field.name)
        val index = names.indexOf(name)
        ColumnExpression(index)
      }
      case ColumnIndex(index) => ColumnExpression(index)

      case b : LBinaryExpression => {
        val l = createPhysicalExpr(b.leftChild(), input)
        val r = createPhysicalExpr(b.rightChild(), input)
        b match {
          case _ : And => AndExpression(l, r)
          case _ : Or => OrExpression(l, r)
          case _ : Eq => EqExpression(l, r)
          case _ : Neq => NeqExpression(l, r)
          case _ : Gt => GtExpression(l, r)
          case _ : Gteq => GteqExpression(l, r)
          case _ : Lt => LtExpression(l, r)
          case _ : Lteq => LteqExpression(l, r)

          case _ : Add => AddExpression(l, r)
          case _ : Subtract => SubtractExpression(l, r)
          case _ : Multiply => MultiplyExpression(l, r)
          case _ : Divide => DivisionExpression(l, r)
        }
      }

      case Sum(expr) => SumExpression(createPhysicalExpr(expr, input))
      case Max(expr) => MaxExpression(createPhysicalExpr(expr, input))
      case Min(expr) => MinExpression(createPhysicalExpr(expr, input))
      case Count(expr) => CountExpression(createPhysicalExpr(expr, input))
    }
  }
}
