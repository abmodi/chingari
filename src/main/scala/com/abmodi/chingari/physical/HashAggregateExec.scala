package com.abmodi.chingari.physical

import com.abmodi.chingari.core.{ColumnBatch, GenericColumnVector, Schema}

import scala.collection.mutable

case class HashAggregateExec(input: PhysicalPlan,
                             groupExp : Seq[Expression],
                             aggExp : Seq[AggregateExpression],
                             schema: Schema) extends PhysicalPlan {

  override def children: Seq[PhysicalPlan] = Seq(input)

  override def output(): Schema = schema

  override def toString: String = s"HashAggregateExec: " +
    s"grouping expressions: ${groupExp.map(_.toString()).mkString(",")};" +
    s"aggregate expressions: ${aggExp.map(_.toString).mkString(",")}"

  override def execute(): Seq[ColumnBatch] = {
    val map = mutable.HashMap[Seq[Any], Seq[Accumulator]]()
    input.execute().foreach(batch => {
      val groupKeys = groupExp.map(_.eval(batch))
      val aggInputVal = aggExp.map(_.inputExpression().eval(batch))

      for (rowIndex <- (0 until batch.numRows)) {
        val rowKeys = groupKeys.map(_.getValue(rowIndex))
        val accumulators = map.getOrElseUpdate(rowKeys, aggExp.map(_.createAccumulator()))

        accumulators.zip(aggInputVal).foreach{case (accumulator, columnVector) => {
          accumulator.accumulate(columnVector.getValue(rowIndex))
        }}
      }
    })

    val arr = Array.ofDim[Any](schema.fields.size, map.size)
    map.zipWithIndex.foreach{case ((keys, accummulators), index) => {
      groupExp.indices.foreach{groupIndex => arr(groupIndex)(index)=keys(groupIndex)}
      aggExp.indices.foreach{aggIndex => arr(aggIndex + groupExp.size)(index) = accummulators(aggIndex).finalValue()}
    }}

    val colVectors = schema.fields.zipWithIndex.map{
      case (field, index) => new GenericColumnVector(field.datatype, arr(index))
    }

    Seq(new ColumnBatch(schema, colVectors))
  }

}
