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
package org.apache.amaterasu.executor.runtime

import org.apache.amaterasu.common.logging.Logging
import org.apache.amaterasu.common.runtime.Environment
import org.apache.spark.SparkContext
import org.apache.spark.sql._

object AmaContext extends Logging {

  var spark: SparkSession = _
  var sc: SparkContext = _
  var jobId: String = _
  var env: Environment = _

  def init(spark: SparkSession,
           jobId: String,
           env: Environment): Unit = {

    AmaContext.spark = spark
    AmaContext.sc = spark.sparkContext
    AmaContext.jobId = jobId
    AmaContext.env = env

  }

  def getDataFrame(actionName: String, dfName: String, format: String = "parquet"): DataFrame = {

    spark.read.format(format).load(s"${env.workingDir}/$jobId/$actionName/$dfName")

  }

  def getDataset[T: Encoder](actionName: String, dfName: String, format: String = "parquet"): Dataset[T] = {

    getDataFrame(actionName, dfName, format).as[T]

  }

}
