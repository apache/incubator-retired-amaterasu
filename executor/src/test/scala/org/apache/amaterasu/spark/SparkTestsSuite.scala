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
import org.apache.amaterasu.common.execution.dependencies._
import org.apache.amaterasu.common.runtime.Environment
import org.apache.amaterasu.executor.common.executors.ProvidersFactory
import org.apache.amaterasu.utilities.TestNotifier
import org.apache.spark.repl.amaterasu.runners.spark.SparkScalaRunner
import org.apache.spark.sql.SparkSession
import org.scalatest._

import scala.collection.mutable.ListBuffer


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

    val conf = Map[String, Any](
      "spark.cassandra.connection.host" -> "127.0.0.1",
      "sourceTable" -> "documents",
      "spark.local.ip" -> "127.0.0.1"
    )
    env.master = "local[1]"
    if (env.configuration != null) env.configuration ++ "pysparkPath" -> "/usr/bin/python" else env.configuration = Map(
      "pysparkPath" -> "/usr/bin/python",
      "cwd" -> resources
    )
    val excEnv = Map[String, Any](
      "PYTHONPATH" -> resources
    )
    env.configuration ++ "spark_exec_env" -> excEnv
    factory = ProvidersFactory(ExecData(env,
      Dependencies(ListBuffer.empty[Repo], List.empty[Artifact]),
      PythonDependencies(List.empty[PythonPackage]),
      Map(
        "spark" -> Map.empty[String, Any],
        "spark_exec_env" -> Map("PYTHONPATH" -> resources))),
      "test",
      new ByteArrayOutputStream(),
      new TestNotifier(),
      "test",
      getClass.getResource("/amaterasu.properties").getPath)
    spark = factory.getRunner("spark", "scala").get.asInstanceOf[SparkScalaRunner].spark

    this.nestedSuites.filter(s => s.isInstanceOf[RunnersLoadingTests]).foreach(s => s.asInstanceOf[RunnersLoadingTests].factory = factory)
    this.nestedSuites.filter(s => s.isInstanceOf[PySparkRunnerTests]).foreach(s => s.asInstanceOf[PySparkRunnerTests].factory = factory)


    super.beforeAll()
  }

  override def afterAll(): Unit = {
    spark.stop()

    super.afterAll()
  }

}
