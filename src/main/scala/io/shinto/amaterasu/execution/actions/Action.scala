package io.shinto.amaterasu.execution.actions

trait Action {

  var nextActionId: Int = _
  var errorActionId: Int = _

  // this is the znode path for the action
  var actionPath: String = _
  var actionId: String = _

  def execute(): Unit

  def handleFailure(attemptNo: Int, e: Exception)

  def announceComplete(): Unit

  def announceStart(): Unit

  protected def announceFailure(): Unit = {}

}