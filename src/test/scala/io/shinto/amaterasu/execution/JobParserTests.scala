package io.shinto.amaterasu.execution

import io.shinto.amaterasu.dsl.JobParser
import org.scalatest.{ Matchers, FlatSpec }

import scala.io.Source

class JobParserTests extends FlatSpec with Matchers {

  "JobParser" should "parse the simple-maki.yaml" in {

    val yaml = Source.fromURL(getClass.getResource("/simple-maki.yaml")).mkString

    val job = JobParser.parse(yaml)
    job.name should be("amaterasu-test")
  }
}
