package com.abmodi.chingari.examples

import com.abmodi.chingari.core._
import com.abmodi.chingari.datasource.CsvDataSource
import com.abmodi.chingari.logical._
import com.abmodi.chingari.physical._


object Example extends App {
  println("Hello World!")
  val schema = Schema(Seq(SchemaField("a", IntegerType), SchemaField("b", IntegerType)))

  val path = "/Users/abmodi/repos/input.csv"
  val csvDataSource = CsvDataSource(path, schema)
  val scanExec = ScanExec(csvDataSource, Seq.empty)
  val filterExec = FilterExec(scanExec, GtExpression(ColumnExpression(0), LiteralIntExpression(3)))
  val projectExec = ProjectExec(filterExec,
                                Schema(Seq(SchemaField("a", IntegerType), SchemaField("b", IntegerType))),
                                Seq(LiteralIntExpression(6),
                                  MultiplyExpression(ColumnExpression(1), LiteralIntExpression(5))))
  val maxAgg = HashAggregateExec(projectExec, Seq(ColumnExpression(0)), Seq(SumExpression(ColumnExpression(1))),
    Schema(Seq(SchemaField("a", IntegerType), SchemaField("max(b)", IntegerType))))
  println(maxAgg.execute().head.toCSV())

  val scan = Scan(csvDataSource, Seq.empty)
  val dataFrame = DataFrame(scan).filter(Gt(Col("a"), Literal(3)))
    .project(Seq(Literal("abc"), Multiply(Literal(5), Col("b"))))
    .aggregate(Seq(Col(0)), Seq(Sum(Col(1))))
  dataFrame.logicalPlan()
  dataFrame.physicalPlan()

  println(dataFrame.execute().head.toCSV())

  val ctx = Context()
  ctx.registerCsv("example", path,
    schema)
  val df = ctx.sql("select sum(a*5) FROM example WHERE b > 2 GROUP BY b * 0")
  println(df.physicalPlan().execute().head.toCSV())

}
