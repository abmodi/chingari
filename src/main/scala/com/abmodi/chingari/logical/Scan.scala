package com.abmodi.chingari.logical

import com.abmodi.chingari.core.Schema
import com.abmodi.chingari.datasource.DataSource

case class Scan(dataSource: DataSource,
                projectionList: Seq[String] = Seq.empty) extends LogicalPlan {

  override def output: Schema = {
    if (projectionList.isEmpty) {
      dataSource.schema
    } else {
      Schema(dataSource.schema.fields.filter(field => projectionList.contains(field.name)))
    }
  }

  override def children: Seq[LogicalPlan] = Seq.empty[LogicalPlan]

  override def toString: String = {
    if (projectionList.isEmpty) {
      "Scan: " + dataSource.toString + "; projection = None"
    } else {
      s"Scan: ${dataSource.toString}; projection = $projectionList"
    }
  }
}
