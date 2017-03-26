package io.shinto.amaterasu.utilities

import io.shinto.amaterasu.common.execution.actions.Notifier
import io.shinto.amaterasu.common.logging.Logging

/**
  * Created by roadan on 10/18/16.
  */
class TestNotifier extends Notifier with Logging {

  override def info(msg: String): Unit = {
    log.info(msg)
  }

  override def success(line: String): Unit = {
    log.info(s"successfully executed line: $line")
  }

  override def error(line: String, msg: String): Unit = {
    log.error(s"Error executing line: $line message: $msg")
  }
}
