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
//package org.apache.amaterasu.spark
//
//import java.io.File
//
//import org.apache.amaterasu.common.runtime._
//import org.apache.amaterasu.common.configuration.ClusterConfig
//import org.apache.amaterasu.utilities.TestNotifier
//
//import scala.collection.JavaConverters._
//import org.apache.commons.io.FileUtils
//import java.io.ByteArrayOutputStream
//
//import org.apache.spark.SparkConf
//import org.apache.spark.repl.Main
//import org.apache.spark.repl.amaterasu.runners.spark.{SparkRunnerHelper, SparkScalaRunner}
//import org.apache.spark.sql.SparkSession
//import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
//
//class SparkScalaRunnerTests extends FlatSpec with Matchers with BeforeAndAfterAll {
//
//  var runner: SparkScalaRunner = _
//
//  override protected def beforeAll(): Unit = {
//
//    FileUtils.deleteQuietly(new File("/tmp/job_5/"))
//
//    val env = Environment()
//    env.workingDir = "file:///tmp"
//    env.master = "local[*]"
//
//
//    val spark = SparkRunnerHelper.createSpark(env, "job_5", Seq.empty[String], Map.empty)
//
//
//    val notifier = new TestNotifier()
//    val strm = new ByteArrayOutputStream()
//    runner = SparkScalaRunner(env, "job_5", spark, strm, notifier, Seq.empty[String])
//    super.beforeAll()
//  }
//
//  "SparkScalaRunner" should "execute the simple-spark.scala" in {
//
//    val script = getClass.getResource("/simple-spark.scala").getPath
//    runner.executeSource(script, "start", Map.empty[String, String].asJava)
//
//  }
//
//  "SparkScalaRunner" should "execute step-2.scala and access data from simple-spark.scala" in {
//
//    val script = getClass.getResource("/step-2.scala").getPath
//    runner.executeSource(script, "cont", Map.empty[String, String].asJava)
//
//  }
//}