package com.abmodi.chingari.core

abstract class DataType {
  type InternalType

  def defaultSize : Int

  def typeName : String

  def simpleString = typeName
}

object DataType {
  def apply(dataType: String): DataType = {
    dataType match {
      case "int" => IntegerType
      case "boolean" => BooleanType
    }
  }
}

class IntegerType extends DataType {
  type InternalType = Int

  override def defaultSize: Int = 4

  override def typeName: String = "int"
}

case object IntegerType extends IntegerType

class BooleanType extends DataType {
  type InternalType = Boolean

  override def defaultSize: Int = 1

  override def typeName: String = "boolean"
}

case object BooleanType extends BooleanType

class LongType extends DataType {
  type InternalType = Long

  override def defaultSize: Int = 8

  override def typeName: String = "long"
}

case object LongType extends LongType

class DoubleType extends DataType {
  type InternalType = Double

  override def defaultSize: Int = 8

  override def typeName: String = "double"
}

case object DoubleType extends DoubleType

class StringType extends DataType {
  type InternalType = String

  override def defaultSize: Int = 100

  override def typeName: String = "string"
}

case object StringType extends StringType