package com.abmodi.chingari.datasource

import com.abmodi.chingari.core.{ColumnBatch, Schema}

trait DataSource {
  def schema: Schema

  def scan(projectList: Seq[String]): Iterator[ColumnBatch]
}
