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
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode, SparkSession}

import scala.reflect.ClassTag

object AmaContext extends Logging {

  var spark: SparkSession = _
  var sc: SparkContext = _
  var jobId: String = _
  var sqlContext: SQLContext = _
  var env: Environment = _

  def init(spark: SparkSession,
           jobId: String,
           env: Environment): Unit = {

    AmaContext.spark = spark
    AmaContext.sqlContext = spark.sqlContext
    AmaContext.sc = spark.sparkContext
    AmaContext.jobId = jobId
    AmaContext.env = env

  }

  def saveDataFrame(df: DataFrame, actionName: String, dfName: String): Unit = {

    try {

      log.debug(s"${env.workingDir}/$jobId/$actionName/$dfName")
      df.write.mode(SaveMode.Overwrite).parquet(s"${env.workingDir}/$jobId/$actionName/$dfName")

    }
    catch {
      case e: Exception =>
        log.error(s"failed storing DataFrame: ${e.getMessage}")

    }
  }

  def saveRDD(rdd: RDD[_], actionName: String, rddName: String): Unit = {

    try {

      log.debug(s"${env.workingDir}/$jobId/$actionName/$rddName")
      rdd.saveAsObjectFile(s"${env.workingDir}/$jobId/$actionName/$rddName")

    }
    catch {
      case e: Exception =>
        log.error(s"failed storing RDD: ${e.getMessage}")

    }

  }

  def getDataFrame(actionName: String, dfName: String): DataFrame = {

    spark.read.parquet(s"${env.workingDir}/$jobId/$actionName/$dfName")

  }

  def getRDD[T: ClassTag](actionName: String, rddName: String): RDD[T] = {

    sc.objectFile[T](s"${env.workingDir}/$jobId/$actionName/$rddName")

  }

  def getActionResult(actionName: String): DataFrame = {

    spark.sql(s"select * from ${AmaContext.jobId}.$actionName")

  }

}
