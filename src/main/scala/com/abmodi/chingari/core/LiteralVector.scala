package com.abmodi.chingari.core

case class LiteralVector(value : Any,
                    dt: DataType,
                    sz : Int) extends ColumnVector {
  override def dataType: DataType = this.dt

  override def getValue(index: Int): Any = value

  override def size: Int = sz
}
