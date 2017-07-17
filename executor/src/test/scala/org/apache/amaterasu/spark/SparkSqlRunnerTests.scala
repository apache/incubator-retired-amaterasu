//package org.apache.amaterasu.spark
//
//import org.apache.amaterasu.common.execution.actions.runners.spark.SparkSqlRunner
//import org.apache.amaterasu.common.runtime.Environment
//import org.apache.amaterasu.executor.runtime.AmaContext
//import org.apache.amaterasu.utilities.TestNotifier
//import org.apache.spark.{SparkConf, SparkContext, SparkEnv}
//import org.apache.log4j.Logger
//import org.apache.log4j.Level
//import org.apache.spark.sql.{SQLContext, SaveMode}
//import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
//
//import scala.io.Source
//
///**
//  * Created by kirupa on 10/12/16.
//  */
//class SparkSqlRunnerTests extends FlatSpec with Matchers with BeforeAndAfterAll {
//
//  Logger.getLogger("org").setLevel(Level.OFF)
//  Logger.getLogger("akka").setLevel(Level.OFF)
//  Logger.getLogger("spark").setLevel(Level.OFF)
//  Logger.getLogger("jetty").setLevel(Level.OFF)
//  Logger.getRootLogger.setLevel(Level.OFF)
//
//
//  val notifier = new TestNotifier()
//
//  var sc: SparkContext = _
//
//  override protected def beforeAll(): Unit = {
//
//    val env = new Environment()
//    env.workingDir = "file:///tmp/"
//
//    val conf = new SparkConf(true)
//      .setMaster("local[*]")
//      .setAppName("sql_job")
//      .set("spark.local.ip", "127.0.0.1")
//
//    sc = new SparkContext("local[*]", "job_5", conf)
// //   sc = new SparkContext(conf)
//    sc.setLogLevel("ERROR")
//
//    println("11111111")
//    println(sc)
//    AmaContext.init(sc, new SQLContext(sc), "sql_job", env)
//    super.beforeAll()
//  }
//
//  override protected def afterAll(): Unit = {
//    sc.stop()
//
//    super.afterAll()
//  }
//
//  "SparkSql" should "load PARQUET data and persist the Data in working directory" in {
//
//    val sparkSql = SparkSqlRunner(AmaContext.env, "spark-sql-parquet", "spark-sql-parquet-action", notifier, sc)
//    sparkSql.executeQuery("temptable", getClass.getResource("/SparkSql/parquet").getPath, "select * from temptable")
//
//  }
//
//  "SparkSql" should "load JSON data and persist the Data in working directory" in {
//
//    val sparkSqlJson = SparkSqlRunner(AmaContext.env, "spark-sql-json", "spark-sql-json-action", notifier, sc)
//    sparkSqlJson.executeQuery("temptable", getClass.getResource("/SparkSql/json/SparkSqlTestData.json").getPath, "select * from temptable")
//
//  }
//
//}
