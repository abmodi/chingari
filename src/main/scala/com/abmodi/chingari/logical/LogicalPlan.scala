package com.abmodi.chingari.logical

import com.abmodi.chingari.core.Schema

abstract class LogicalPlan {
  def output: Schema

  def children : Seq[LogicalPlan]

  def pretty: String = format(this)

  private def format(plan : LogicalPlan, indent : Int = 0) : String = {
    val sb = new StringBuilder()
    for (i <- (0 until indent)) {
      sb.append("\t")
    }
    sb.append(plan).append("\n")
    plan.children.foreach({child => sb.append(format(child, indent + 1))})
    sb.toString()
  }
}

