package com.abmodi.chingari.datasource

import com.abmodi.chingari.core.{BooleanType, ColumnBatch, ColumnVector, DoubleType, GenericColumnVector, IntegerType, LongType, Schema, SchemaField, StringType}
import com.univocity.parsers.common.record.Record

import java.io.{File, FileNotFoundException}
import com.univocity.parsers.csv._

import scala.collection.mutable.ArrayBuffer


case class CsvDataSource(filePath : String,
                         schema : Schema,
                        ) extends DataSource {

  class ColumnBatchIterator(parser : CsvParser) extends Iterator[ColumnBatch] {
    private var nextBatch : Option[ColumnBatch] = None
    private var started = false

    override def hasNext: Boolean = {
      if (!started) {
        started = true
        nextBatch = fetchNextBatch()
      }
      nextBatch.isDefined
    }

    override def next(): ColumnBatch = {
      if (!started) {
        hasNext
      }
      if (nextBatch.isEmpty) {
        throw new RuntimeException("Can't read past the end of iterator")
      }
      val out = nextBatch.get
      nextBatch = fetchNextBatch()
      out
    }

    private def fetchNextBatch() : Option[ColumnBatch] = {
      val rows = ArrayBuffer[Record]()
      var line: Record = null
      do {
        line = parser.parseNextRecord()
        if (line != null) {
          rows.append(line)
        }
      } while (line != null && rows.size < BATCH_SIZE)

      if (rows.isEmpty) {
        None
      } else {
        Option(createBatch(rows, schema))
      }
    }

    private def createBatch(rows: ArrayBuffer[Record],
                            schema: Schema): ColumnBatch = {
      val colVectors = schema.fields.map(createColVector(_, rows))
      new ColumnBatch(schema, colVectors)
    }

    private def createColVector(schemaField: SchemaField, rows : ArrayBuffer[Record]) : ColumnVector = {
      val arr = Array.ofDim[Any](rows.size)
      rows.zipWithIndex.foreach(tuple => {
        val row = tuple._1
        val index = tuple._2
        schemaField.datatype match {
          case IntegerType => {
            val int = row.getInt(schemaField.name)
            arr(index) = int
          }
          case BooleanType => {
            val bool = row.getBoolean(schemaField.name)
            arr(index) = bool
          }

          case LongType => {
            val long = row.getLong(schemaField.name)
            arr(index) = long
          }

          case DoubleType => {
            val double = row.getDouble(schemaField.name)
            arr(index) = double
          }

          case StringType => {
            val string = row.getString(schemaField.name)
            arr(index) = string
          }
        }
      })
      new GenericColumnVector(schemaField.datatype, arr)
    }
  }

  override def scan(projectList: Seq[String]): Iterator[ColumnBatch] = {
    val file = new File(filePath)
    if (!file.exists()) {
      throw new FileNotFoundException(s"file not preset at $filePath")
    }

    val readSchema = if (projectList.isEmpty) {
      schema
    } else {
      schema.select(projectList)
    }

    val settings = defaultSettings()
    settings.setHeaders(schema.fields.map(_.name):_*)

    val parser = new CsvParser(settings)
    parser.beginParsing(file)
    parser.getDetectedFormat
    new ColumnBatchIterator(parser)
  }



  private def defaultSettings() : CsvParserSettings = {
    val settings = new CsvParserSettings()
    settings.setDelimiterDetectionEnabled(true)
    settings.setLineSeparatorDetectionEnabled(true)
    settings.setSkipEmptyLines(true)
    settings.setAutoClosingEnabled(true)
    settings
  }
}
