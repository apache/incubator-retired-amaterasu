package io.shinto.amaterasu.executor.execution.actions.runners.spark

import java.io.ByteArrayOutputStream

import io.shinto.amaterasu.common.execution.actions.Notifier
import io.shinto.amaterasu.common.logging.Logging
import io.shinto.amaterasu.common.runtime.Environment
import org.apache.spark.SparkContext

/**
  * Created by eyalbenivri on 02/09/2016.
  */
class SparkRRunner extends Logging with IAmaRunner {
  override def initializeAmaContext(env: Environment): Unit = {

  }

  override def executeSource(actionSource: String, actionName: String): Unit = {

  }
}

object SparkRRunner {
  def apply(
    env: Environment,
    jobId: String,
    sparkContext: SparkContext,
    outStream: ByteArrayOutputStream,
    notifier: Notifier,
    jars: Seq[String]
  ): SparkRRunner = {
    new SparkRRunner()
  }
}