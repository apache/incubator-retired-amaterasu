package io.shinto.amaterasu.common.dataobjects

import org.joda.time.DateTime

case class JobData(src: String, branch: String = "master", id: String, timeCreated: DateTime, startTime: DateTime, endTime: DateTime)