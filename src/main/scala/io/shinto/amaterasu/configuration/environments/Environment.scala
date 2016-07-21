package io.shinto.amaterasu.configuration.environments

case class Environment() {

  var name: String = ""
  var master: String = ""

  var inputRootPath: String = ""
  var outputRootPath: String = ""
  var workingDir: String = ""

  var configuration: Map[String, Any] = null

}
