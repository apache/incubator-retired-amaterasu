package io.shinto.amaterasu.executor.configuration.environments

import java.io.FileInputStream

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import io.shinto.amaterasu.common.runtime.Environment

/**
  * Created by roadan on 5/23/16.
  */
object GitEnvironmentLoader {

  def getEnv(name: String) = {

    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)

    mapper.readValue(new FileInputStream(s"repo/env/$name.json"), Environment.getClass)

  }

}
