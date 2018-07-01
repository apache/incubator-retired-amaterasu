/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.amaterasu.framework.spark.runner.repl

import org.apache.amaterasu.executor.common.executors.ProvidersFactory
import org.apache.amaterasu.framework.spark.runtime.AmaContext
import org.scalatest.{BeforeAndAfterAll, DoNotDiscover, FlatSpec, Matchers}

import scala.collection.JavaConverters._
import scala.io.Source

@DoNotDiscover
class SparkScalaRunnerTests extends FlatSpec with Matchers with BeforeAndAfterAll {
  var factory: ProvidersFactory = _
  var runner: SparkScalaRunner = _


  "SparkScalaRunner" should "execute the simple-spark.scala" in {


    val sparkRunner =factory.getRunner("spark", "scala").get.asInstanceOf[SparkScalaRunner]
    val script = getClass.getResource("/simple-spark.scala").getPath
    val sourceCode = Source.fromFile(script).getLines().mkString("\n")
    sparkRunner.executeSource(sourceCode, "start", Map.empty[String, String].asJava)

  }

  "SparkScalaRunner" should "execute step-2.scala and access data from simple-spark.scala" in {

    val sparkRunner =factory.getRunner("spark", "scala").get.asInstanceOf[SparkScalaRunner]
    val script = getClass.getResource("/step-2.scala").getPath
    sparkRunner.env.workingDir = s"${getClass.getResource("/tmp").getPath}"
    AmaContext.init(sparkRunner.spark,"job",sparkRunner.env)
    val sourceCode = Source.fromFile(script).getLines().mkString("\n")
    sparkRunner.executeSource(sourceCode, "cont", Map.empty[String, String].asJava)

  }


}