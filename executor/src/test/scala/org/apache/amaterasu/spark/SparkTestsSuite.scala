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
package org.apache.amaterasu.spark

import java.io.{ByteArrayOutputStream, File}

import org.apache.amaterasu.RunnersTests.RunnersLoadingTests
import org.apache.amaterasu.common.dataobjects.ExecData
import org.apache.amaterasu.common.execution.dependencies.{Artifact, Dependencies, Repo}
import org.apache.amaterasu.common.runtime.Environment
import org.apache.amaterasu.utilities.TestNotifier
import org.apache.amaterasu.executor.mesos.executors.ProvidersFactory
import org.apache.spark.repl.amaterasu.runners.spark.SparkScalaRunner
import org.apache.spark.sql.SparkSession
import org.scalatest._

import scala.collection.mutable.ListBuffer

/**
  * Created by roadan on 22/6/17.
  */
class SparkTestsSuite extends Suites(
  new PySparkRunnerTests(),
  new RunnersLoadingTests()) with BeforeAndAfterAll {

  var env: Environment = _
  var factory: ProvidersFactory = _
  var spark: SparkSession = _

  override def beforeAll(): Unit = {

    env = Environment()
    env.workingDir = "file:///tmp/"
    env.master = "local[*]"

    // I can't apologise enough for this
    val resources = new File(getClass.getResource("/spark_intp.py").getPath).getParent
    factory = ProvidersFactory(ExecData(env, Dependencies(ListBuffer.empty[Repo], List.empty[Artifact]), Map("spark" -> Map.empty[String, Any],"spark_exec_env"->Map("PYTHONPATH"->resources))), "test", new ByteArrayOutputStream(), new TestNotifier(), "test")
    spark = factory.getRunner("spark", "scala").get.asInstanceOf[SparkScalaRunner].spark

    this.nestedSuites.filter(s => s.isInstanceOf[RunnersLoadingTests]).foreach(s => s.asInstanceOf[RunnersLoadingTests].factory = factory)
    this.nestedSuites.filter(s => s.isInstanceOf[PySparkRunnerTests]).foreach(s => {
      s.asInstanceOf[PySparkRunnerTests].spark = spark
      s.asInstanceOf[PySparkRunnerTests].env = env
    })

    super.beforeAll()
  }

  override def afterAll(): Unit = {
    spark.stop()

    super.afterAll()
  }

}
