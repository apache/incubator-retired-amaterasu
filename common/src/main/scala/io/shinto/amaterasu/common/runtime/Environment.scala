package io.shinto.amaterasu.common.runtime

/**
  * Created by roadan on 8/20/16.
  */
case class Environment() {

  var name: String = _
  var master: String = _

  var inputRootPath: String = _
  var outputRootPath: String = _
  var workingDir: String = _

  var configuration: Map[String, String] = _

}