package com.abmodi.chingari.core

class ColumnBatch(val schema: Schema,
                  val columnVectors: Seq[ColumnVector]) {
  def numRows = columnVectors.head.size

  def numCols = columnVectors.size

  def getColumn(index : Int) = columnVectors(index)

  def toCSV() : String = {
    val sb = new StringBuilder()
    (0 until numRows).foreach { rowIndex =>
      (0 until numCols).foreach { colIndex =>
        if (colIndex > 0) {
          sb.append(",")
        }
        sb.append(getColumn(colIndex).getValue(rowIndex))
      }
      sb.append("\n")
    }
    sb.toString()
  }

  override def toString: String = toCSV()
}
