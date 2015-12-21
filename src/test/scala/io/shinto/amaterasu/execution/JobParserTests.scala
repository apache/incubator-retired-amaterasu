package io.shinto.amaterasu.execution

import io.shinto.amaterasu.dsl.JobParser
import org.scalatest.{ Matchers, FlatSpec }

import scala.io.Source

class JobParserTests extends FlatSpec with Matchers {

  val yaml = Source.fromURL(getClass.getResource("/simple-maki.yaml")).mkString
  val job = JobParser.parse(yaml)

  "JobParser" should "parse the simple-maki.yaml" in {

    job.name should be("amaterasu-test")

  }

  //TODO: I suspect this test is not indicative, need to verify this
  it should "also have two actions in the right order" in {

    job.flow.size() should be (2)
    job.flow.get(0).data.name should be("start")
    job.flow.get(1).data.name should be("step2")

  }
}
