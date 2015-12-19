package io.shinto.amaterasu.execution.actions

trait Action {
  def execute(): Unit
  def handleFailure(attemptNo: Int, e: Exception)
  def announceComplete(): Unit
  def announceStart(): Unit
  protected def announceFailure(): Unit = {}
}
