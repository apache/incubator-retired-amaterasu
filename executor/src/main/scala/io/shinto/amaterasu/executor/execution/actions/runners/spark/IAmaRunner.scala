package io.shinto.amaterasu.executor.execution.actions.runners.spark

import io.shinto.amaterasu.common.runtime.Environment

/**
  * Created by eyalbenivri on 07/09/2016.
  */
trait IAmaRunner {
  def initializeAmaContext(env: Environment): Unit
  def executeSource(actionSource: String, actionName: String): Unit
}
