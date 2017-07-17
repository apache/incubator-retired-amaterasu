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

import java.io.File

import org.apache.amaterasu.common.runtime.Environment
import org.apache.amaterasu.utilities.TestNotifier
import org.apache.amaterasu.executor.execution.actions.runners.spark.PySpark.PySparkRunner
import org.apache.spark.sql.SparkSession
import org.scalatest._

import scala.collection.JavaConverters._

@DoNotDiscover
class PySparkRunnerTests extends FlatSpec with Matchers with BeforeAndAfterAll {


  var runner: PySparkRunner = _
  var spark: SparkSession = _
  var env: Environment = _

  def delete(file: File) {
    if (file.isDirectory)
      Option(file.listFiles).map(_.toList).getOrElse(Nil).foreach(delete(_))
    file.delete
  }

  override protected def beforeAll(): Unit = {


    val notifier = new TestNotifier()

    // this is an ugly hack, getClass.getResource("/").getPath should have worked but
    // stopped working when we moved to gradle :(
    val resources = new File(getClass.getResource("/spark_intp.py").getPath).getParent

    runner = PySparkRunner(env, "job_5", notifier, spark, resources)

    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    spark.stop()
    val pysparkDir = new File(getClass.getResource("/pyspark").getPath)
    val py4jDir = new File(getClass.getResource("/py4j").getPath)
    delete(pysparkDir)
    delete(py4jDir)
    spark.stop()

    super.afterAll()
  }

  "PySparkRunner.executeSource" should "execute simple python code" in {
    runner.executeSource(getClass.getResource("/simple-python.py").getPath, "test_action1", Map.empty[String, String].asJava)
  }

  it should "print and trows an errors" in {
    a[java.lang.Exception] should be thrownBy {
      runner.executeSource(getClass.getResource("/simple-python-err.py").getPath, "test_action2", Map.empty[String, String].asJava)
    }
  }

  it should "also execute spark code written in python" in {
    runner.executeSource(getClass.getResource("/simple-pyspark.py").getPath, "test_action3", Map.empty[String, String].asJava)
  }

  it should "also execute spark code written in python with AmaContext being used" in {
    runner.executeSource(getClass.getResource("/pyspark-with-amacontext.py").getPath, "test_action4", Map.empty[String, String].asJava)
  }

}