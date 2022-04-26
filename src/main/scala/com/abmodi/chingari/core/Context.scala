package com.abmodi.chingari.core

import com.abmodi.chingari.datasource.CsvDataSource
import com.abmodi.chingari.logical.Scan
import com.abmodi.chingari.sql.SParser

import scala.collection.mutable

case class Context() {
  private val tables = new mutable.HashMap[String, DataFrame]()

  def sql(sql : String) : DataFrame = {
    DataFrame(SParser(sql, tables.toMap))
  }

  def csv(filePath : String, schema: Schema) : DataFrame = {
    DataFrame(Scan(CsvDataSource(filePath, schema)))
  }

  def registerCsv(tablename : String, filePath : String, schema: Schema): Unit = {
    tables += tablename -> csv(filePath, schema)
  }
}
