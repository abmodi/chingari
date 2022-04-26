package com.abmodi.chingari.core

class GenericColumnVector(dt: DataType, val values: Array[Any]) extends ColumnVector {
  override def dataType: DataType = dt

  override def getValue(index: Int): Any = values(index)

  override def size: Int = values.size
}
