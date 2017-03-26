package io.shinto.amaterasu.spark

import io.shinto.amaterasu.common.execution.actions.runners.spark.SparkSqlRunner
import io.shinto.amaterasu.common.runtime.Environment
import io.shinto.amaterasu.executor.runtime.AmaContext
import io.shinto.amaterasu.utilities.TestNotifier
import org.apache.spark.{SparkConf, SparkContext, SparkEnv}
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.{SQLContext, SaveMode}
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

  val env = new Environment()
  env.workingDir = "file:///tmp/"

  val notifier = new TestNotifier()

  var sc: SparkContext = _

  override protected def beforeAll(): Unit = {
    val conf = new SparkConf(true)
      .setMaster("local[*]")
      .setAppName("sql_job")

    sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    sc.stop()

    super.afterAll()
  }

  "SparkSql" should "load PARQUET data and persist the Data in working directory" in {

    val sparkSql = SparkSqlRunner(env, "spark-sql-parquet", "spark-sql-parquet-action", notifier, sc)
    AmaContext.init(sc, new SQLContext(sc), "spark-sql-parquet", env)
    sparkSql.executeQuery("temptable", getClass.getResource("/SparkSql/parquet").getPath, "select * from temptable")

  }

  "SparkSql" should "load JSON data and persist the Data in working directory" in {

    val sparkSqlJson = SparkSqlRunner(env, "spark-sql-json", "spark-sql-json-action", notifier, sc)
    AmaContext.init(sc, new SQLContext(sc), "spark-sql-json", env)
    sparkSqlJson.executeQuery("temptable", getClass.getResource("/SparkSql/json/SparkSqlTestData.json").getPath, "select * from temptable")

  }

}
