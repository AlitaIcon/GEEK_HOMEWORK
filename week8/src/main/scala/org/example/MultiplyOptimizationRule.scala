package org.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal, Multiply}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.types.Decimal

case class MultiplyOptimizationRule(spark: SparkSession) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = plan transformAllExpressions {
    case Multiply(left, right, _) if isOne(right) =>
      logWarning("Right is one, MultiplyOptimizationRule take effect")
      left
    case Multiply(left, right, _) if isOne(left) =>
      logWarning("Left is one, MultiplyOptimizationRule take effect")
      right
  }

  // 判断是否为整数1或者小数的1
  def isOne(expression: Expression): Boolean = {
    expression.isInstanceOf[Literal] &&
      ((expression.asInstanceOf[Literal].value.isInstanceOf[Int] && expression.asInstanceOf[Literal].value.asInstanceOf[Int] == 1)
        || (expression.asInstanceOf[Literal].value.isInstanceOf[Decimal] && expression.asInstanceOf[Literal].value.asInstanceOf[Decimal].toDouble == 1.0))
  }
}
