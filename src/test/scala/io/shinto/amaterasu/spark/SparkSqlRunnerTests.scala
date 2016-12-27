package io.shinto.amaterasu.spark

import io.shinto.amaterasu.runtime.{ AmaContext, Environment }
import io.shinto.amaterasu.execution.actions.runners.spark.SparkSqlRunner
import io.shinto.amaterasu.utilities.TestNotifier
import org.apache.spark.{ SparkConf, SparkContext, SparkEnv }
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.{ SQLContext, SaveMode }
import org.scalatest.{ FlatSpec, Matchers }

import scala.io.Source

/**
  * Created by kirupa on 10/12/16.
  */
class SparkSqlRunnerTests extends FlatSpec with Matchers {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  Logger.getLogger("spark").setLevel(Level.OFF)
  Logger.getLogger("jetty").setLevel(Level.OFF)
  Logger.getRootLogger.setLevel(Level.OFF)

  val env = new Environment()

  env.workingDir = "file:///tmp"

  env.master = "local[*]"

  val notifier = new TestNotifier()

  val conf = new SparkConf(true)
    .setMaster(env.master)
    .setAppName("job_sparksql")

  val sc = new SparkContext(conf)

  sc.setLogLevel("ERROR")

  val sparkSql = SparkSqlRunner(env, "spark-sql-job1", "spark-sql-action", notifier, sc)

  "SparkSql" should "run SparkSql and persist the Data in working directory" in {

    sparkSql.executeQuery("temptable", getClass.getResource("/SparkSql/SparkSqlTestData.json").getPath, "select * from temptable")

  }

  //sc.stop()
}
