package io.shinto.amaterasu.configuration

import java.io.InputStream
import java.util.Properties

class SparkConfig {

  var sparkExecutorUri: String = "http://www.apache.org/dyn/closer.lua/spark/spark-1.6.1/spark-1.6.1-bin-hadoop2.6.tgz"

  def load(file: InputStream): Unit = {
    val props: Properties = new Properties()

    props.load(file)
    file.close()

    if (props.containsKey("sparkExecutorUri")) sparkExecutorUri = props.getProperty("sparkExecutorUri")
  }

}
