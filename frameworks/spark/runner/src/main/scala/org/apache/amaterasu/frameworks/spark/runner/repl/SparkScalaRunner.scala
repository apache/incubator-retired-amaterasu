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
package org.apache.amaterasu.frameworks.spark.runner.repl

import java.io.ByteArrayOutputStream
import java.util

import org.apache.amaterasu.common.execution.actions.Notifier
import org.apache.amaterasu.common.logging.Logging
import org.apache.amaterasu.common.runtime.Environment
import org.apache.amaterasu.frameworks.spark.runtime.AmaContext
import org.apache.amaterasu.sdk.AmaterasuRunner
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.io.Source
import scala.tools.nsc.interpreter.{IMain, Results}

class ResHolder(var value: Any)

class SparkScalaRunner(var env: Environment,
                       var jobId: String,
                       var interpreter: IMain,
                       var outStream: ByteArrayOutputStream,
                       var spark: SparkSession,
                       var notifier: Notifier,
                       var jars: Seq[String]) extends Logging with AmaterasuRunner {

  private def scalaOptionError(msg: String): Unit = {
    notifier.error("", msg)
  }

  override def getIdentifier = "scala"

  val holder = new ResHolder(null)

  override def executeSource(actionSource: String, actionName: String, exports: util.Map[String, String]): Unit = {
    val source = Source.fromString(actionSource)
    interpretSources(source, actionName, exports.asScala.toMap)
  }

  def interpretSources(source: Source, actionName: String, exports: Map[String, String]): Unit = {

    notifier.info(s"================= Started action $actionName =================")
    //notifier.info(s"exports is: $exports")

    for (line <- source.getLines()) {

      // ignoring empty or commented lines
      if (!line.isEmpty && !line.trim.startsWith("*") && !line.startsWith("/")) {

        outStream.reset()
        log.debug(line)

        if (line.startsWith("import")) {
          interpreter.interpret(line)
        }
        else {
          val intresult = interpreter.interpret(line)

          val result = interpreter.prevRequestList.last.lineRep.call("$result")

          // intresult: Success, Error, etc
          // result: the actual result (RDD, df, etc.) for caching
          // outStream.toString gives you the error message
          intresult match {
            case Results.Success =>
              log.debug("Results.Success")

              notifier.success(line)

              val resultName = interpreter.prevRequestList.last.termNames.last

              if (exports.contains(resultName.toString)) {

                val format = exports(resultName.toString)

                if (result != null) {

                  result match {
                    case ds: Dataset[_] =>
                      log.debug(s"persisting DataFrame: $resultName")
                      val writeLine = s"""$resultName.write.mode(SaveMode.Overwrite).format("$format").save("${env.workingDir}/$jobId/$actionName/$resultName")"""
                      val writeResult = interpreter.interpret(writeLine)
                      if (writeResult != Results.Success) {
                        val err = outStream.toString
                        notifier.error(writeLine, err)
                        log.error(s"error persisting dataset: $writeLine Failed with: $err")
                        //throw new Exception(err)
                      }
                      log.debug(s"persisted DataFrame: $resultName")

                    case _ => notifier.info(s"""+++> result type ${result.getClass}""")
                  }
                }
              }

            case Results.Error =>
              log.debug("Results.Error")
              val err = outStream.toString
              notifier.error(line, err)
              throw new Exception(err)

            case Results.Incomplete =>
              log.debug("Results.Incomplete")

          }
        }
      }
    }

    notifier.info(s"================= finished action $actionName =================")
  }

  def initializeAmaContext(env: Environment): Unit = {

    // setting up some context :)
    val sc = this.spark.sparkContext
    val sqlContext = this.spark.sqlContext

    interpreter.interpret("import scala.util.control.Exception._")
    interpreter.interpret("import org.apache.spark.{ SparkContext, SparkConf }")
    interpreter.interpret("import org.apache.spark.sql.SQLContext")
    interpreter.interpret("import org.apache.spark.sql.{ Dataset, SparkSession }")
    interpreter.interpret("import org.apache.spark.sql.SaveMode")
    interpreter.interpret("import org.apache.amaterasu.frameworks.spark.runtime.AmaContext")
    interpreter.interpret("import org.apache.amaterasu.common.runtime.Environment")

    // creating a map (_contextStore) to hold the different spark contexts
    // in th REPL and getting a reference to it
    interpreter.interpret("var _contextStore = scala.collection.mutable.Map[String, AnyRef]()")
    val contextStore = interpreter.prevRequestList.last.lineRep.call("$result").asInstanceOf[mutable.Map[String, AnyRef]]
    AmaContext.init(spark, jobId, env)

    // populating the contextStore
    contextStore.put("sc", sc)
    contextStore.put("sqlContext", sqlContext)
    contextStore.put("env", env)
    contextStore.put("spark", spark)
    contextStore.put("ac", AmaContext)

    interpreter.interpret("val sc = _contextStore(\"sc\").asInstanceOf[SparkContext]")
    interpreter.interpret("val sqlContext = _contextStore(\"sqlContext\").asInstanceOf[SQLContext]")
    interpreter.interpret("val env = _contextStore(\"env\").asInstanceOf[Environment]")
    interpreter.interpret("val spark = _contextStore(\"spark\").asInstanceOf[SparkSession]")
    interpreter.interpret("val AmaContext = _contextStore(\"ac\").asInstanceOf[AmaContext]")
    interpreter.interpret("import sqlContext.implicits._")

    // initializing the AmaContext
    println(s"""AmaContext.init(sc, sqlContext ,"$jobId")""")

  }

}

object SparkScalaRunner extends Logging {

  def apply(env: Environment,
            jobId: String,
            spark: SparkSession,
            outStream: ByteArrayOutputStream,
            notifier: Notifier,
            jars: Seq[String]): SparkScalaRunner = {

    new SparkScalaRunner(env, jobId, SparkRunnerHelper.getOrCreateScalaInterperter(outStream, jars), outStream, spark, notifier, jars)

  }

}
