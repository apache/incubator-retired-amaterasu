package io.shinto.amaterasu.common.configuration

import java.io.InputStream
import java.util.Properties

class SparkConfig {
  val config = new ClusterConfig()
  var sparkExecutorUri: String = s"http://${sys.env("AMA_NODE")}:${config.Webserver.Port}/dist/spark-${config.Webserver.sparkVersion}.tgz"

  def load(file: InputStream): Unit = {
    val props: Properties = new Properties()

    props.load(file)
    file.close()

    if (props.containsKey("sparkExecutorUri")) sparkExecutorUri = props.getProperty("sparkExecutorUri")
  }

}
