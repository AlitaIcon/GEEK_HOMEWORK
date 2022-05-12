package org.example

import org.apache.spark.sql.SparkSessionExtensions

class MySparkSessionExtensions extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectOptimizerRule {
      session => MultiplyOptimizationRule(session);
    }
  }
}
