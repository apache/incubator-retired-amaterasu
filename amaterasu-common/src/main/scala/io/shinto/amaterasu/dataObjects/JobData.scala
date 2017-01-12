package io.shinto.amaterasu.dataObjects

import org.joda.time.DateTime

case class JobData(src: String, branch: String = "master", id: String, timeCreated: DateTime, startTime: DateTime, endTime: DateTime)