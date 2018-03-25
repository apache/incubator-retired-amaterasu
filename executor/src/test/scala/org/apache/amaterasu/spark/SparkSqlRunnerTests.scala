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
import org.apache.amaterasu.executor.common.executors.ProvidersFactory
import org.apache.amaterasu.executor.execution.actions.runners.spark.SparkSql.SparkSqlRunner
import org.apache.amaterasu.executor.runtime.AmaContext
import org.apache.amaterasu.utilities.TestNotifier
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.scalatest.{BeforeAndAfterAll, DoNotDiscover, FlatSpec, Matchers}

import scala.collection.JavaConverters._

/**
  * Created by kirupa on 10/12/16.
  */
@DoNotDiscover
class SparkSqlRunnerTests extends FlatSpec with Matchers with BeforeAndAfterAll {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  Logger.getLogger("spark").setLevel(Level.OFF)
  Logger.getLogger("jetty").setLevel(Level.OFF)
  Logger.getRootLogger.setLevel(Level.OFF)


  val notifier = new TestNotifier()

  var factory: ProvidersFactory = _
  var env: Environment = _


//  override protected def beforeAll(): Unit = {
//
//    val env = Environment()
//    env.workingDir = "file:/tmp/"
//    spark = SparkSession.builder()
//      .appName("sql-job")
//      .master("local[*]")
//      .config("spark.local.ip", "127.0.0.1")
//      .getOrCreate()
//
//    AmaContext.init(spark, "sql-job", env)
//
//    super.beforeAll()
//  }
//
//  override protected def afterAll(): Unit = {
//    this.spark.sparkContext.stop()
//    super.afterAll()
//  }


  /*
  Test whether parquet is used as default file format to load data from previous actions
   */

  "SparkSql" should "load data as parquet if no input foramt is specified" in {

    val sparkSql: SparkSqlRunner = factory.getRunner("spark", "sql").get.asInstanceOf[SparkSqlRunner]
    val spark: SparkSession = sparkSql.spark

    //Prepare test dataset
    val inputDf = spark.read.parquet(getClass.getResource("/SparkSql/parquet").getPath)
    inputDf.write.mode(SaveMode.Overwrite).parquet(s"${env.workingDir}/${sparkSql.jobId}/sparkSqlDefaultParquetJobAction/sparkSqlDefaultParquetJobActionTempDf")
    sparkSql.executeSource("select * FROM AMACONTEXT_sparkSqlDefaultParquetJobAction_sparkSqlDefaultParquetJobActionTempDf where age=22", "sql_parquet_test", Map("result" -> "parquet").asJava)
    val outputDf = spark.read.parquet(s"${env.workingDir}/${sparkSql.jobId}/sql_parquet_test/result")
    println("Output Default Parquet: " + inputDf.count + "," + outputDf.first().getString(1))
    outputDf.first().getString(1) shouldEqual "Michael"
  }

  /*
  Test whether the parquet data is successfully parsed, loaded and processed by SparkSQL
   */

  "SparkSql" should "load PARQUET data directly from previous action's dataframe and persist the Data in working directory" in {

    val sparkSql: SparkSqlRunner = factory.getRunner("spark", "sql").get.asInstanceOf[SparkSqlRunner]
    val spark: SparkSession = sparkSql.spark

    //Prepare test dataset
    val inputDf = spark.read.parquet(getClass.getResource("/SparkSql/parquet").getPath)
    inputDf.write.mode(SaveMode.Overwrite).parquet(s"${env.workingDir}/${sparkSql.jobId}/sparkSqlParquetJobAction/sparkSqlParquetJobActionTempDf")
    sparkSql.executeSource("select * FROM AMACONTEXT_sparkSqlParquetJobAction_sparkSqlParquetJobActionTempDf READAS parquet", "sql_parquet_test", Map("result2" -> "parquet").asJava)
    val outputDf = spark.read.parquet(s"${env.workingDir}/${sparkSql.jobId}/sql_parquet_test/result2")
    println("Output Parquet: " + inputDf.count + "," + outputDf.count)
    inputDf.first().getString(1) shouldEqual outputDf.first().getString(1)
  }


  /*
  Test whether the JSON data is successfully parsed, loaded by SparkSQL
  */

  "SparkSql" should "load JSON data directly from previous action's dataframe and persist the Data in working directory" in {

    val sparkSql: SparkSqlRunner = factory.getRunner("spark", "sql").get.asInstanceOf[SparkSqlRunner]
    val spark: SparkSession = sparkSql.spark

    //Prepare test dataset
    val inputDf = spark.read.json(getClass.getResource("/SparkSql/json").getPath)
    inputDf.write.mode(SaveMode.Overwrite).json(s"${env.workingDir}/${sparkSql.jobId}/sparkSqlJsonJobAction/sparkSqlJsonJobActionTempDf")
    sparkSql.executeSource("select * FROM amacontext_sparkSqlJsonJobAction_sparkSqlJsonJobActionTempDf  where age='30' READAS json", "sql_json_test", Map("result" -> "json").asJava)
    val outputDf = spark.read.json(s"${env.workingDir}/${sparkSql.jobId}/sql_json_test/result")
    println("Output JSON: " + inputDf.count + "," + outputDf.count)
    outputDf.first().getString(1) shouldEqual "Kirupa"

  }

  /*
  Test whether the CSV data is successfully parsed, loaded by SparkSQL
  */

  "SparkSql" should "load CSV data directly from previous action's dataframe and persist the Data in working directory" in {

    val sparkSql: SparkSqlRunner = factory.getRunner("spark", "sql").get.asInstanceOf[SparkSqlRunner]
    val spark: SparkSession = sparkSql.spark

    //Prepare test dataset
    val inputDf = spark.read.csv(getClass.getResource("/SparkSql/csv").getPath)
    inputDf.write.mode(SaveMode.Overwrite).csv(s"${env.workingDir}/${sparkSql.jobId}/sparkSqlCsvJobAction/sparkSqlCsvJobActionTempDf")
    sparkSql.executeSource("select * FROM amacontext_sparkSqlCsvJobAction_sparkSqlCsvJobActionTempDf READAS csv", "sql_csv_test", Map("result" -> "csv").asJava)

    val outputDf = spark.read.csv(s"${env.workingDir}/${sparkSql.jobId}/sql_csv_test/result")
    println("Output CSV: " + inputDf.count + "," + outputDf.count)
    inputDf.first().getString(1) shouldEqual outputDf.first().getString(1)
  }

  /*
  Test whether the data can be directly read from a file and executed by sparkSql
  */
//  "SparkSql" should "load data directly from a file and persist the Data in working directory" in {
//
//    val tempFileEnv = Environment()
//    tempFileEnv.workingDir = "file:/tmp/"
//    AmaContext.init(spark, "sparkSqlFileJob", tempFileEnv)
//
//    val sparkSql: SparkSqlRunner = SparkSqlRunner(AmaContext.env, "sparkSqlFileJob", notifier, spark)
//    sparkSql.executeSource("SELECT * FROM parquet.`" + getClass.getResource("/SparkSql/parquet").getPath + "`", "sql_parquet_file_test", Map("result" -> "parquet").asJava)
//    val outputParquetDf = spark.read.parquet(s"${tempFileEnv.workingDir}/sparkSqlFileJob/sql_parquet_file_test/result")
//    println("Output Parquet dataframe: " + outputParquetDf.show)
//    outputParquetDf.first().getString(1) shouldEqual "Michael"
//    sparkSql.executeSource("SELECT * FROM json.`" + getClass.getResource("/SparkSql/json").getPath + "`","sql_parquet_file_test", Map("result" -> "json").asJava)
//
//    val outputJsonDf = spark.read.parquet(s"${tempFileEnv.workingDir}/sparkSqlFileJob/sql_parquet_file_test/result")
//    println("Output Json dataframe: " + outputJsonDf.show)
//    outputJsonDf.first().getString(1) shouldEqual "Sampath"
//
//  }


}
