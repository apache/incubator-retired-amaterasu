package io.shinto.amaterasu.executor.execution.actions.runners.spark.PySpark

import io.shinto.amaterasu.executor.execution.actions.runners.spark.PySpark.ResultType.ResultType

/**
  * Created by roadan on 10/17/16.
  */
object ResultType extends Enumeration {
  type ResultType = Value
  val success = Value("success")
  val error = Value("error")
  val completion = Value("completion")
}

case class PySparkResult(
  resultType: ResultType,
  action: String,
  statement: String,
  message: String
)
