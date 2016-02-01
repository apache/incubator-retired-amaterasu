package io.shinto.amaterasu.configuration

import java.io.InputStream
import java.util.Properties

class SparkConfig {

  var master: String = "local[*]"

  def load(file: InputStream): Unit = {
    val props: Properties = new Properties()

    props.load(file)
    file.close()

    if (props.containsKey("aws.accessKeyId")) master = props.getProperty("aws.accessKeyId")
  }

}
