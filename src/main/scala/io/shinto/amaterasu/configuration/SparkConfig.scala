package io.shinto.amaterasu.configuration

import java.io.InputStream
import java.util.Properties

class SparkConfig {

  var sparkExecutorUri: String = s"http://${sys.env("AMA_NODE")}:8000/spark-1.6.1-2.tgz"

  def load(file: InputStream): Unit = {
    val props: Properties = new Properties()

    props.load(file)
    file.close()

    if (props.containsKey("sparkExecutorUri")) sparkExecutorUri = props.getProperty("sparkExecutorUri")
  }

}
