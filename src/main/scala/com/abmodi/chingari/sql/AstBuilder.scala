package com.abmodi.chingari.sql

import com.abmodi.chingari.core.DataFrame
import com.abmodi.chingari.logical._
import org.antlr.v4.runtime.tree.TerminalNode

import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

class AstBuilder(tables : Map[String, DataFrame]) extends SqlBaseVisitor[AnyRef] {
  override def visitSingleStatement(ctx: SqlParser.SingleStatementContext): LogicalPlan = {
    visit(ctx.statement()).asInstanceOf[LogicalPlan]
  }

  override def visitStatement(ctx: SqlParser.StatementContext): LogicalPlan = {
    val plan = visitFromClause(ctx.fromClause())
    val filteredPlan = if (ctx.whereClause() != null) withWhereClause(ctx.whereClause, plan) else plan

    val projectList = ctx.selectClause().namedExpressionSeq().
      namedExpression().map(visit(_).asInstanceOf[LogicalExpression]).toSeq

    if (ctx.aggregationClause() != null) {
      withAggregationClause(ctx.aggregationClause(), filteredPlan, projectList)
    } else {
      Project(filteredPlan, projectList)
    }
  }

  private def withWhereClause(ctx: SqlParser.WhereClauseContext, plan : LogicalPlan) : LogicalPlan = {
    val filterExpression = visit(ctx.booleanExpression()).asInstanceOf[LogicalExpression]
    Filter(plan, filterExpression)
  }

  private def withAggregationClause(ctx : SqlParser.AggregationClauseContext,
                                    plan : LogicalPlan,
                                    projectList : Seq[LogicalExpression]) : LogicalPlan = {
    val groupingExpressions = ctx.groupingExpressions.map(visit(_).asInstanceOf[LogicalExpression])
    val aggregateExpressions = projectList.filter(_.isInstanceOf[AggregateExpression]).
      map(_.asInstanceOf[AggregateExpression])
    Aggregate(plan, aggregateExpressions, groupingExpressions.toSeq)
  }

  override def visitFromClause(ctx: SqlParser.FromClauseContext): LogicalPlan = {
    visit(ctx.tableIdentifier()).asInstanceOf[LogicalPlan]
  }

  override def visitStringLiteral(ctx: SqlParser.StringLiteralContext): LogicalExpression = {
    val rawStrippedQualifier = ctx.getText.substring(0, ctx.getText.length)
    LiteralString(rawStrippedQualifier)
  }

  override def visitDoubleLiteral(ctx: SqlParser.DoubleLiteralContext): LogicalExpression = {
    val rawStrippedQualifier = ctx.getText.substring(0, ctx.getText.length)
    LiteralDouble(rawStrippedQualifier.toDouble)
  }

  override def visitBooleanLiteral(ctx: SqlParser.BooleanLiteralContext): LogicalExpression = {
    val rawStrippedQualifier = ctx.getText.substring(0, ctx.getText.length)
    LiteralBoolean(rawStrippedQualifier.toBoolean)
  }

  override def visitIntLiteral(ctx: SqlParser.IntLiteralContext): LogicalExpression = {
    val rawStrippedQualifier = ctx.getText.substring(0, ctx.getText.length)
    LiteralInt(rawStrippedQualifier.toInt)
  }

  override def visitLongLiteral(ctx: SqlParser.LongLiteralContext): LogicalExpression = {
    val rawStrippedQualifier = ctx.getText.substring(0, ctx.getText.length - 1)
    LiteralLong(rawStrippedQualifier.toLong)
  }

  override def visitNumericLiteral(ctx: SqlParser.NumericLiteralContext): LogicalExpression = {
    visit(ctx.number()).asInstanceOf[LogicalExpression]
  }

  override def visitColumnReference(ctx: SqlParser.ColumnReferenceContext): LogicalExpression = {
    val rawStrippedQualifier = ctx.getText.substring(0, ctx.getText.length)
    Column(rawStrippedQualifier)
  }

  override def visitArithmeticBinary(ctx: SqlParser.ArithmeticBinaryContext): LogicalExpression = {
    val left = visit(ctx.left).asInstanceOf[LogicalExpression]
    val right = visit(ctx.right).asInstanceOf[LogicalExpression]

    ctx.operator.getType match {
      case SqlParser.ASTERISK => Multiply(left, right)
      case SqlParser.SLASH => Divide(left, right)
      case SqlParser.PLUS => Add(left, right)
      case SqlParser.MINUS => Subtract(left, right)
    }
  }

  override def visitLogicalBinary(ctx: SqlParser.LogicalBinaryContext): LogicalExpression = {
    val left = visit(ctx.left).asInstanceOf[LogicalExpression]
    val right = visit(ctx.right).asInstanceOf[LogicalExpression]

    ctx.operator.getType match {
      case SqlParser.AND => And(left, right)
      case SqlParser.OR => Or(left, right)
    }
  }

  override def visitCompariosn(ctx: SqlParser.CompariosnContext): LogicalExpression = {
    val left = visit(ctx.left).asInstanceOf[LogicalExpression]
    val right = visit(ctx.right).asInstanceOf[LogicalExpression]

    val operator = ctx.comparisonOperator().getChild(0).asInstanceOf[TerminalNode]
    operator.getSymbol.getType match {
      case SqlParser.EQ => Eq(left, right)
      case SqlParser.NEQ => Neq(left, right)
      case SqlParser.GT => Gt(left, right)
      case SqlParser.GTE => Gteq(left, right)
      case SqlParser.LT => Lt(left, right)
      case SqlParser.LTE => Lteq(left, right)
    }
  }

  override def visitTableIdentifier(ctx: SqlParser.TableIdentifierContext): LogicalPlan = {
    val table = tables.get(ctx.getText)
    if (table.isEmpty) {
      throw new RuntimeException(s"Table not found ${ctx.getText}")
    }
    table.get.plan
  }

  override def visitMaxAggregate(ctx: SqlParser.MaxAggregateContext): AggregateExpression = {
    Max(visit(ctx.child).asInstanceOf[LogicalExpression])
  }

  override def visitMinAggregate(ctx: SqlParser.MinAggregateContext): AggregateExpression = {
    Min(visit(ctx.child).asInstanceOf[LogicalExpression])
  }

  override def visitSumAggregate(ctx: SqlParser.SumAggregateContext): AggregateExpression = {
    Sum(visit(ctx.child).asInstanceOf[LogicalExpression])
  }

  override def visitCountAggregate(ctx: SqlParser.CountAggregateContext): AggregateExpression = {
    Count(visit(ctx.child).asInstanceOf[LogicalExpression])
  }
}
