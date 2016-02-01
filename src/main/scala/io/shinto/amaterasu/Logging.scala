package io.shinto.amaterasu

import org.slf4j.LoggerFactory

trait Logging {
  protected lazy val log = LoggerFactory.getLogger(getClass.getName)
}

