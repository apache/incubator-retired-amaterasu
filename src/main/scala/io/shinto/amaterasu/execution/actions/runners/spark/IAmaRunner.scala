package io.shinto.amaterasu.execution.actions.runners.spark

import io.shinto.amaterasu.runtime.Environment

/**
  * Created by eyalbenivri on 07/09/2016.
  */
trait IAmaRunner {
  def initializeAmaContext(env: Environment):Unit
  def executeSource(actionSource: String, actionName: String): Unit
}
