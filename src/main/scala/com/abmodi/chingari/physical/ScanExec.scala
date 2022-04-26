package com.abmodi.chingari.physical

import com.abmodi.chingari.core.{ColumnBatch, Schema}
import com.abmodi.chingari.datasource.DataSource

case class ScanExec(ds: DataSource,
                    projectionList: Seq[String]) extends PhysicalPlan {

  override def output(): Schema = ds.schema.select(projectionList)

  override def children: Seq[PhysicalPlan] = Seq.empty

  override def execute(): Seq[ColumnBatch] = ds.scan(projectionList)

  override def toString: String = s"ScanExec: $ds"
}
