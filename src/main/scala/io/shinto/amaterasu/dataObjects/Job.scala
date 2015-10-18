package io.shinto.amaterasu.dataObjects

import com.github.nscala_time.time.Imports._

case class Job(src: String, id: String, timeCreated: DateTime, startTime: DateTime, endTime: DateTime)