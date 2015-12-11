package io.shinto.amaterasu.execution

import java.util.concurrent.{ LinkedBlockingQueue, BlockingQueue }
import io.shinto.amaterasu.dataObjects.{ JobData, ActionData }

/**
  * Created by roadan on 12/5/15.
  */
class JobManager {

  private var jobsQueue: BlockingQueue[ActionData] = null

  //def setScriptPart
}

object JobManager {

  def apply(data: JobData): JobManager = {

    val manager = new JobManager()
    manager.jobsQueue = new LinkedBlockingQueue[ActionData]()

    manager
  }

}