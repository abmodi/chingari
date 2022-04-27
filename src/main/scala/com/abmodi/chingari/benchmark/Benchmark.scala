package com.abmodi.chingari.benchmark

import com.abmodi.chingari.core.{Context, DoubleType, IntegerType, Schema, SchemaField, StringType}

case object Benchmark {
  def apply(setup : () => Context, query : String) : Unit = {
    val cc = setup()
    val startTime = System.nanoTime()
    println(cc.sql(query).execute().next().toCSV())
    val timeDiff = System.nanoTime() - startTime
    println(s"Time taken : $timeDiff")
  }
}

object NyTaxiBenchmark {
  val setup : () => Context = () => {
    val cc = Context()
    val path = "/Users/abmodi/repos/taxi.csv"
    val schema = Schema(Seq(SchemaField("vendorId", IntegerType),
      SchemaField("pickupDate", StringType),
      SchemaField("dropoffDate", StringType),
      SchemaField("passengerCount", IntegerType),
      SchemaField("distance", DoubleType),
      SchemaField("rateCodeId", IntegerType),
      SchemaField("storeFlag", StringType),
      SchemaField("pickUpLocationId", IntegerType),
      SchemaField("dropOffLocationId", IntegerType),
      SchemaField("paymentType", IntegerType),
      SchemaField("fareAmount", DoubleType),
      SchemaField("extra", DoubleType),
      SchemaField("tax", DoubleType),
      SchemaField("tip", DoubleType),
      SchemaField("toll", DoubleType),
      SchemaField("surcharge", DoubleType),
      SchemaField("total", DoubleType)))
    cc.registerCsv("taxi", path, schema)
    cc
  }
}

object main extends App {
  Benchmark(NyTaxiBenchmark.setup, "select count(total) from taxi group by storeFlag")
}
