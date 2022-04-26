package com.abmodi.chingari.logical

import com.abmodi.chingari.core.Schema

case class Project(input: LogicalPlan,
                   projectList: Seq[LogicalExpression]) extends LogicalPlan {
  override def output: Schema = Schema(projectList.map(_.output(input)))

  override def children: Seq[LogicalPlan] = Seq(input)

  override def toString: String = s"Project ${projectList.map(_.toString).mkString(",")}"
}
