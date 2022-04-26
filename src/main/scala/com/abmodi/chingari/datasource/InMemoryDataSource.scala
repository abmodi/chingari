package com.abmodi.chingari.datasource

import com.abmodi.chingari.core.{ColumnBatch, Schema}

case class InMemoryDataSource(sch: Schema,
                              data: Seq[ColumnBatch]) extends DataSource {
  override def schema: Schema = sch

  override def scan(projectList: Seq[String]): Seq[ColumnBatch] = {
    val projectionIndices = projectList.map {
      sch.fields.map(_.name).indexOf(_)
    }
    val newSchema = Schema(sch.fields.filter(field => projectList.contains(field.name)))
    val projectedBatch = data.map(batch => {
      new ColumnBatch(newSchema, projectionIndices.map(batch.getColumn))
    })
    projectedBatch
  }

  override def toString: String = "InMemoryDataSource"
}
