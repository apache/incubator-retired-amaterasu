package org.apache.amaterasu.executor.yarn.executors

import org.apache.amaterasu.common.execution.actions.Notifier
import org.apache.amaterasu.common.logging.Logging
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.ipc.YarnRPC

class YarnNotifier(conf: YarnConfiguration) extends Notifier with Logging {

  var rpc: YarnRPC = YarnRPC.create(conf)

  override def info(msg: String): Unit = {
    log.info(s"""-> ${msg}""")
  }

  override def success(line: String): Unit = {
    log.info(s"""SUCCESS: ${line}""")
  }

  override def error(line: String, msg: String): Unit = {
    log.error(s"""ERROR: ${line}: ${msg}""")
  }
}
