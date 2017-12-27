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
package org.apache.amaterasu.executor.execution.actions.runners.spark

import java.io.ByteArrayOutputStream
import java.util

import org.apache.amaterasu.common.execution.actions.Notifier
import org.apache.amaterasu.common.logging.Logging
import org.apache.amaterasu.common.runtime.Environment
import org.apache.amaterasu.sdk.AmaterasuRunner
import org.apache.spark.SparkContext


class SparkRRunner extends Logging with AmaterasuRunner {

  override def getIdentifier = "spark-r"

  override def executeSource(actionSource: String, actionName: String, exports: util.Map[String, String]): Unit = {
  }
}

object SparkRRunner {
  def apply(
    env: Environment,
    jobId: String,
    sparkContext: SparkContext,
    outStream: ByteArrayOutputStream,
    notifier: Notifier,
    jars: Seq[String]
  ): SparkRRunner = {
    new SparkRRunner()
  }
}