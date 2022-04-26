package com.abmodi.chingari.physical

import com.abmodi.chingari.core.{ColumnBatch, Schema}

abstract class PhysicalPlan {
  def output() :Schema

  def execute() : Seq[ColumnBatch]

  def children : Seq[PhysicalPlan]

  def pretty() : String = format(this)

  private def format(plan : PhysicalPlan, indent : Int = 0) : String = {
    val sb = new StringBuilder()
    for (i <- (0 until indent)) {
      sb.append("\t")
    }
    sb.append(plan).append("\n")
    plan.children.foreach({child => sb.append(format(child, indent + 1))})
    sb.toString()
  }
}
