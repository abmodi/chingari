package com.abmodi.chingari.core

case class Schema(fields : Seq[SchemaField]) {
  def select(projectionList : Seq[String]) = {
    Schema(fields.filter(field => projectionList.contains(field.name)))
  }
}

case class SchemaField(name: String,
                       datatype: DataType)
