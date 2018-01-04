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
      .appName("sql-job")
      .master("local[*]")
      .config("spark.local.ip", "127.0.0.1")
      .getOrCreate()

    AmaContext.init(spark, "sql-job", env)
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    this.spark.sparkContext.stop()
    super.afterAll()
  }


  /*
  Test whether parquet is used as default file format to load data from previous actions
   */

  "SparkSql" should "load data as parquet if no input foramt is specified" in {

    val defaultParquetEnv = new Environment()
    defaultParquetEnv.workingDir = "file:/tmp/"
    AmaContext.init(spark, "sparkSqlDefaultParquetJob", defaultParquetEnv)
    //Prepare test dataset
    val inputDf = spark.read.parquet(getClass.getResource("/SparkSql/parquet").getPath)
    inputDf.write.mode(SaveMode.Overwrite).parquet(s"${defaultParquetEnv.workingDir}/sparkSqlDefaultParquetJob/sparkSqlDefaultParquetJobAction/sparkSqlDefaultParquetJobActionTempDf")
    val sparkSql: SparkSqlRunner = SparkSqlRunner(AmaContext.env, "sparkSqlDefaultParquetJob", "sparkSqlDefaultParquetJobAction", notifier, spark)
    sparkSql.executeQuery("select * FROM AMACONTEXT_sparkSqlDefaultParquetJobAction_sparkSqlDefaultParquetJobActionTempDf where age=22")
    val outputDf = spark.read.parquet(s"${defaultParquetEnv.workingDir}/sparkSqlDefaultParquetJob/sparkSqlDefaultParquetJobAction/sparkSqlDefaultParquetJobActionDf")
    println("Output Default Parquet: "+inputDf.count + "," + outputDf.first().getString(1))
    outputDf.first().getString(1) shouldEqual("Michael")
  }

  /*
  Test whether the parquet data is successfully parsed, loaded and processed by SparkSQL
   */

  "SparkSql" should "load PARQUET data directly from previous action's dataframe and persist the Data in working directory" in {

    val tempParquetEnv = new Environment()
    tempParquetEnv.workingDir = "file:/tmp/"
    AmaContext.init(spark, "sparkSqlParquetJob", tempParquetEnv)
    //Prepare test dataset
    val inputDf = spark.read.parquet(getClass.getResource("/SparkSql/parquet").getPath)
    inputDf.write.mode(SaveMode.Overwrite).parquet(s"${tempParquetEnv.workingDir}/sparkSqlParquetJob/sparkSqlParquetJobAction/sparkSqlParquetJobActionTempDf")
    val sparkSql: SparkSqlRunner = SparkSqlRunner(AmaContext.env, "sparkSqlParquetJob", "sparkSqlParquetJobAction", notifier, spark)
    sparkSql.executeQuery("select * FROM AMACONTEXT_sparkSqlParquetJobAction_sparkSqlParquetJobActionTempDf READAS parquet")
    val outputDf = spark.read.parquet(s"${tempParquetEnv.workingDir}/sparkSqlParquetJob/sparkSqlParquetJobAction/sparkSqlParquetJobActionDf")
    println("Output Parquet: "+inputDf.count + "," + outputDf.count)
    inputDf.first().getString(1) shouldEqual(outputDf.first().getString(1))
  }


  /*
  Test whether the JSON data is successfully parsed, loaded by SparkSQL
  */

  "SparkSql" should "load JSON data directly from previous action's dataframe and persist the Data in working directory" in {

    val tempJsonEnv = new Environment()
    tempJsonEnv.workingDir = "file:/tmp/"
    AmaContext.init(spark, "sparkSqlJsonJob", tempJsonEnv)
    //Prepare test dataset
    val inputDf = spark.read.json(getClass.getResource("/SparkSql/json").getPath)
    inputDf.write.mode(SaveMode.Overwrite).json(s"${tempJsonEnv.workingDir}/sparkSqlJsonJob/sparkSqlJsonJobAction/sparkSqlJsonJobActionTempDf")
    val sparkSql: SparkSqlRunner = SparkSqlRunner(AmaContext.env, "sparkSqlJsonJob", "sparkSqlJsonJobAction", notifier, spark)
    sparkSql.executeQuery("select * FROM amacontext_sparkSqlJsonJobAction_sparkSqlJsonJobActionTempDf  where age='30' READAS json")
    val outputDf = spark.read.json(s"${tempJsonEnv.workingDir}/sparkSqlJsonJob/sparkSqlJsonJobAction/sparkSqlJsonJobActionDf")
    println("Output JSON: "+inputDf.count + "," + outputDf.count)
    outputDf.first().getString(1) shouldEqual("Kirupa")

  }

  /*
  Test whether the CSV data is successfully parsed, loaded by SparkSQL
  */

  "SparkSql" should "load CSV data directly from previous action's dataframe and persist the Data in working directory" in {

    val tempCsvEnv = new Environment()
    tempCsvEnv.workingDir = "file:/tmp/"
    AmaContext.init(spark, "sparkSqlCsvJob", tempCsvEnv)
    //Prepare test dataset
    val inputDf = spark.read.csv(getClass.getResource("/SparkSql/csv").getPath)
    inputDf.write.mode(SaveMode.Overwrite).csv(s"${tempCsvEnv.workingDir}/sparkSqlCsvJob/sparkSqlCsvJobAction/sparkSqlCsvJobActionTempDf")
    val sparkSql: SparkSqlRunner = SparkSqlRunner(AmaContext.env, "sparkSqlCsvJob", "sparkSqlCsvJobAction", notifier, spark)
    sparkSql.executeQuery("select * FROM amacontext_sparkSqlCsvJobAction_sparkSqlCsvJobActionTempDf READAS csv")
    val outputDf = spark.read.csv(s"${tempCsvEnv.workingDir}/sparkSqlCsvJob/sparkSqlCsvJobAction/sparkSqlCsvJobActionDf")
    println("Output CSV: "+inputDf.count + "," + outputDf.count)
    inputDf.first().getString(1) shouldEqual(outputDf.first().getString(1))
  }
  /*
  Test whether the data can be directly read from a file and executed by sparkSql
  */

  "SparkSql" should "load data directly from a file and persist the Data in working directory" in {

    val tempFileEnv = new Environment()
    tempFileEnv.workingDir = "file:/tmp/"
    AmaContext.init(spark, "sparkSqlFileJob", tempFileEnv)

    val sparkSql: SparkSqlRunner = SparkSqlRunner(AmaContext.env, "sparkSqlFileJob", "sparkSqlFileJobAction", notifier, spark)
    sparkSql.executeQuery("SELECT * FROM parquet.`"+getClass.getResource("/SparkSql/parquet").getPath+"`")
    val outputParquetDf = spark.read.parquet(s"${tempFileEnv.workingDir}/sparkSqlFileJob/sparkSqlFileJobAction/sparkSqlFileJobActionDf")
    println("Output Parquet dataframe: "+ outputParquetDf.show)
    outputParquetDf.first().getString(1) shouldEqual("Michael")
    sparkSql.executeQuery("SELECT * FROM json.`"+getClass.getResource("/SparkSql/json").getPath+"`")
    //@TODO: change the below read.parquet to read.outputFileFormat specified in the yaml file
    val outputJsonDf = spark.read.parquet(s"${tempFileEnv.workingDir}/sparkSqlFileJob/sparkSqlFileJobAction/sparkSqlFileJobActionDf")
    println("Output Json dataframe: "+ outputJsonDf.show)
    outputJsonDf.first().getString(1) shouldEqual("Sampath")

  }


}
