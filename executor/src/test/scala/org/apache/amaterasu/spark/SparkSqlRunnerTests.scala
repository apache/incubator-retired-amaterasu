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
import org.apache.amaterasu.common.runtime.Environment
import org.apache.amaterasu.executor.execution.actions.runners.spark.SparkSql.SparkSqlRunner
import org.apache.amaterasu.executor.runtime.AmaContext
import org.apache.amaterasu.utilities.TestNotifier
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.io.Source

/**
  * Created by kirupa on 10/12/16.
  */
class SparkSqlRunnerTests extends FlatSpec with Matchers with BeforeAndAfterAll {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  Logger.getLogger("spark").setLevel(Level.OFF)
  Logger.getLogger("jetty").setLevel(Level.OFF)
  Logger.getRootLogger.setLevel(Level.OFF)


  val notifier = new TestNotifier()

  var spark: SparkSession = _

  override protected def beforeAll(): Unit = {

    val env = new Environment()
    env.workingDir = "file:/tmp/"
    spark = SparkSession.builder()
      .appName("sql_job")
      .master("local[*]")
      .config("spark.local.ip", "127.0.0.1")
      .getOrCreate()

    AmaContext.init(spark,"sql_job",env)
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    this.spark.sparkContext.stop()
    super.afterAll()
  }


  /*
  Test whether the parquet data is successfully loaded and processed by SparkSQL
   */
  "SparkSql" should "load PARQUET data and persist the Data in working directory" in {

    val sparkSql:SparkSqlRunner = SparkSqlRunner(AmaContext.env, "spark-sql-parquet", "spark-sql-parquet-action", notifier, spark)
    sparkSql.executeQuery("temptable", getClass.getResource("/SparkSql/parquet").getPath, "select * from temptable")

  }


  /*
  Test whether the JSON data is successfully loaded by SparkSQL
   */

  "SparkSql" should "load JSON data and persist the Data in working directory" in {

    val sparkSqlJson = SparkSqlRunner(AmaContext.env, "spark-sql-json", "spark-sql-json-action", notifier, spark)
    sparkSqlJson.executeQuery("temptable", getClass.getResource("/SparkSql/json/SparkSqlTestData.json").getPath, "select * from temptable")

  }

}
