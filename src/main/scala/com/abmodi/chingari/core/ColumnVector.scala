package com.abmodi.chingari.core

trait ColumnVector {
  def dataType: DataType

  def getValue(index: Int) : Any

  def size: Int
}
