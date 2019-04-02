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
package org.apache.amaterasu.frameworks.spark.runner

import java.io.{ByteArrayOutputStream, File}

import org.apache.amaterasu.common.dataobjects.{Artifact, ExecData, Repo}
import org.apache.amaterasu.common.execution.dependencies._
import org.apache.amaterasu.common.runtime.Environment
import org.apache.amaterasu.executor.common.executors.ProvidersFactory
import org.apache.amaterasu.frameworks.spark.runner.pyspark.PySparkRunnerTests
import org.apache.amaterasu.frameworks.spark.runner.repl.{SparkScalaRunner, SparkScalaRunnerTests}
import org.apache.amaterasu.frameworks.spark.runner.sparksql.SparkSqlRunnerTests
import org.apache.amaterasu.utilities.TestNotifier
import org.apache.spark.sql.SparkSession
import org.scalatest._

import scala.collection.mutable.ListBuffer
import scala.collection.JavaConverters._

class SparkTestsSuite extends Suites(
  //new PySparkRunnerTests,
  new RunnersLoadingTests,
  new SparkSqlRunnerTests,
  new SparkScalaRunnerTests
) with BeforeAndAfterAll {

  var env: Environment = _
  var factory: ProvidersFactory = _
  var spark: SparkSession = _

  private def createTestMiniconda(): Unit = {
    println(s"PATH: ${new File(".").getAbsolutePath}")
    new File("miniconda/pkgs").mkdirs()
  }

  override def beforeAll(): Unit = {

    // I can't apologise enough for this
    val resources = new File(getClass.getResource("/spark_intp.py").getPath).getParent
    val workDir = new File(resources).getParentFile.getParent

    env = new Environment()
    env.setWorkingDir(s"file://$workDir")

    env.setMaster("local[1]")
    if (env.getConfiguration != null) env.setConfiguration(Map("pysparkPath" -> "/usr/bin/python").asJava) else env.setConfiguration( Map(
      "pysparkPath" -> "/usr/bin/python",
      "cwd" -> resources
    ).asJava)

    val excEnv = Map[String, Any](
      "PYTHONPATH" -> resources
    )
    createTestMiniconda()
    env.setConfiguration(Map( "spark_exec_env" -> excEnv).asJava)
    factory = ProvidersFactory(new ExecData(env,
      new Dependencies(ListBuffer.empty[Repo].asJava, List.empty[Artifact].asJava),
      new PythonDependencies(List.empty[PythonPackage].asJava),
      Map(
        "spark" -> Map.empty[String, Any].asJava,
        "spark_exec_env" -> Map("PYTHONPATH" -> resources).asJava).asJava),
      "test",
      new ByteArrayOutputStream(),
      new TestNotifier(),
      "test",
      "localhost",
      getClass.getClassLoader.getResource("amaterasu.properties").getPath)
    spark = factory.getRunner("spark", "scala").get.asInstanceOf[SparkScalaRunner].spark

    this.nestedSuites.filter(s => s.isInstanceOf[RunnersLoadingTests]).foreach(s => s.asInstanceOf[RunnersLoadingTests].factory = factory)
    this.nestedSuites.filter(s => s.isInstanceOf[PySparkRunnerTests]).foreach(s => s.asInstanceOf[PySparkRunnerTests].factory = factory)
    this.nestedSuites.filter(s => s.isInstanceOf[SparkSqlRunnerTests]).foreach(s => s.asInstanceOf[SparkSqlRunnerTests].factory = factory)
    this.nestedSuites.filter(s => s.isInstanceOf[SparkScalaRunnerTests]).foreach(s => s.asInstanceOf[SparkScalaRunnerTests].factory = factory)
    this.nestedSuites.filter(s => s.isInstanceOf[SparkSqlRunnerTests]).foreach(s => s.asInstanceOf[SparkSqlRunnerTests].env = env)

    super.beforeAll()
  }

  override def afterAll(): Unit = {
    new File("miniconda").delete()
    spark.stop()

    super.afterAll()
  }

}
