package io.shinto.amaterasu.enums

object ActionStatus extends Enumeration {
  type ActionStatus = Value
  val pending = Value("pending")
  val queued = Value("queued")
  val started = Value("started")
  val complete = Value("complete")
  val failed = Value("failed")
  val canceled = Value("canceled")
}