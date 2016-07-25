package io.shinto.amaterasu.configuration

import java.io.InputStream
import java.util.Properties

class SparkConfig {

  var sparkExecutorUri: String = "http://127.0.0.1:8000/spark-1.6.1-1.tgz"

  def load(file: InputStream): Unit = {
    val props: Properties = new Properties()

    props.load(file)
    file.close()

    if (props.containsKey("sparkExecutorUri")) sparkExecutorUri = props.getProperty("sparkExecutorUri")
  }

}
