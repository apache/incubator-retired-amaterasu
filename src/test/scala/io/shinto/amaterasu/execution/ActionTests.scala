package io.shinto.amaterasu.execution

import java.util.concurrent.LinkedBlockingQueue

import io.shinto.amaterasu.Config
import io.shinto.amaterasu.dataObjects.ActionData
import io.shinto.amaterasu.execution.actions.{ SequentialAction }
import org.scalatest.{ Matchers, FlatSpec }

class ActionTests extends FlatSpec with Matchers {

  "an Action" should "queue it's ActionData when executed" in {

    val queue = new LinkedBlockingQueue[ActionData]()
    val config = Config()
    val jobId = s"job_${System.currentTimeMillis()}"
    val data = ActionData("test_action", "http://github.com", "master", "", "1", jobId)
    val action = SequentialAction(data, config, queue)

    action.execute()
    queue.peek() should be(data)

  }

}
