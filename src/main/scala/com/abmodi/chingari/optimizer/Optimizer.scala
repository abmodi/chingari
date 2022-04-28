package com.abmodi.chingari.optimizer

import com.abmodi.chingari.logical.LogicalPlan

class Optimizer {
  def optimize(plan : LogicalPlan) : LogicalPlan = {
    val rules = Seq(ProjectionPushDown)
    var currentPlan = plan
    rules.foreach(rule => currentPlan = rule.apply(currentPlan))
    currentPlan
  }
}

object Optimizer {
  def apply(): Optimizer = new Optimizer
}

abstract class OptimizerRule {

  val ruleName : String = {
    getClass.getName
  }

  def apply(plan : LogicalPlan) : LogicalPlan
}

