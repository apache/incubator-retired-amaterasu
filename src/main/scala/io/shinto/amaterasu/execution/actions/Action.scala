package io.shinto.amaterasu.execution.actions

/**
  * Created by roadan on 12/11/15.
  */
trait Action {
  def execute(): Unit
  def handleFailure(attemptNo: Int)
  def announceComplete(): Unit
  def announceStart(): Unit
  protected def announceFailure(): Unit = {}
}
