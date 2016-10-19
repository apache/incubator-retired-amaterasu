package io.shinto.amaterasu.runtime

/**
  * Created by roadan on 8/20/16.
  */
case class Environment() {

  var name: String = ""
  var master: String = ""

  var inputRootPath: String = ""
  var outputRootPath: String = ""
  var workingDir: String = ""

  var configuration: Map[String, Any] = null
  var configuration: Map[String, String] = null

}
