/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.amaterasu.common.configuration

import java.io.{File, InputStream}
import java.nio.file.Paths
import java.util.Properties

import org.apache.amaterasu.common.logging.Logging
import org.apache.commons.configuration.ConfigurationException

import scala.collection.mutable

class ClusterConfig extends Logging {

  val DEFAULT_FILE: InputStream = getClass.getResourceAsStream("/src/main/scripts/amaterasu.properties")
  //val DEFAULT_FILE = getClass().getResourceAsStream("/amaterasu.properties")
  var version: String = ""
  var user: String = ""
  var zk: String = ""
  var mode: String = ""
  var master: String = "127.0.0.1"
  var masterPort: String = "5050"
  var timeout: Double = 600000
  var taskMem: Int = 1024
  var distLocation: String = "local"
  var workingFolder: String = ""
  // TODO: get rid of hard-coded version
  var pysparkPath: String = _
  var Jar: String = _
  var JarName: String = _
  // the additionalClassPath is currently for testing purposes, when amaterasu is
  // not packaged, there is a need to load the spark-assembly jar
  var additionalClassPath: String = ""
  var spark: Spark = new Spark()

  //this should be a filesystem path that is reachable by all executors (HDFS, S3, local)

  class YARN {

    var queue: String = "default"
    var hdfsJarsPath: String = ""
    var master: Master = new Master()
    var hadoopHomeDir: String = ""

    def load(props: Properties): Unit = {
      if (props.containsKey("yarn.queue")) queue = props.getProperty("yarn.queue")
      if (props.containsKey("yarn.jarspath")) hdfsJarsPath = props.getProperty("yarn.jarspath")
      if (props.containsKey("yarn.hadoop.home.dir")) hadoopHomeDir = props.getProperty("yarn.hadoop.home.dir")

      this.master.load(props)
      this.Worker.load(props)
    }

    class Master {
      var cores: Int = 1
      var memoryMB: Int = 1024

      def load(props: Properties): Unit = {
        if (props.containsKey("yarn.master.cores")) this.cores = props.getProperty("yarn.master.cores").toInt
        if (props.containsKey("yarn.master.memoryMB")) this.memoryMB = props.getProperty("yarn.master.memoryMB").toInt
      }
    }

    val Master = new Master()

    object Worker {
      var cores: Int = 1
      var memoryMB: Int = 1024

      def load(props: Properties): Unit = {
        if (props.containsKey("yarn.worker.cores")) this.cores = props.getProperty("yarn.worker.cores").toInt
        if (props.containsKey("yarn.worker.memoryMB")) this.memoryMB = props.getProperty("yarn.worker.memoryMB").toInt
      }
    }


  }


  val YARN = new YARN()

  class Spark {
    var home: String = ""
    var opts: mutable.Map[String, String] = mutable.Map()

    def load(props: Properties): Unit = {
      if (props.containsKey("spark.home")) home = props.getProperty("spark.home")
      import scala.collection.JavaConversions._
      for (key <- props.propertyNames()) {
        if (key.toString.startsWith("spark.opts.")) {
          val value = props.getProperty(key.toString)
          val newKey = key.toString.replace("spark.opts.", "")
          opts.put(newKey, value)
        }
      }
    }
  }


  object Webserver {
    var Port: String = ""
    var Root: String = ""
    var sparkVersion: String = ""

    def load(props: Properties): Unit = {

      if (props.containsKey("webserver.port")) Webserver.Port = props.getProperty("webserver.port")
      if (props.containsKey("webserver.root")) Webserver.Root = props.getProperty("webserver.root")
      if (props.containsKey("spark.version")) Webserver.sparkVersion = props.getProperty("spark.version")
    }
  }

  object Jobs {

    var cpus: Double = 1
    var mem: Long = 1024
    var repoSize: Long = 1024

    def load(props: Properties): Unit = {

      if (props.containsKey("jobs.cpu")) cpus = props.getProperty("jobs.cpu").toDouble
      if (props.containsKey("jobs.mem")) mem = props.getProperty("jobs.mem").toLong
      if (props.containsKey("jobs.repoSize")) repoSize = props.getProperty("jobs.repoSize").toLong

      Tasks.load(props)
    }

    object Tasks {

      var attempts: Int = 3
      var cpus: Int = 1
      var mem: Int = 1024

      def load(props: Properties): Unit = {

        if (props.containsKey("jobs.tasks.attempts")) attempts = props.getProperty("jobs.tasks.attempts").toInt
        if (props.containsKey("jobs.tasks.cpus")) cpus = props.getProperty("jobs.tasks.cpus").toInt
        if (props.containsKey("jobs.tasks.mem")) mem = props.getProperty("jobs.tasks.mem").toInt

      }
    }

  }

  object AWS {

    var accessKeyId: String = ""
    var secretAccessKey: String = ""
    var distBucket: String = ""
    var distFolder: String = ""

    def load(props: Properties): Unit = {

      if (props.containsKey("aws.accessKeyId")) accessKeyId = props.getProperty("aws.accessKeyId")
      if (props.containsKey("aws.secretAccessKey")) secretAccessKey = props.getProperty("aws.secretAccessKey")
      if (props.containsKey("aws.distBucket")) distBucket = props.getProperty("aws.distBucket")
      if (props.containsKey("aws.distFolder")) distFolder = props.getProperty("aws.distFolder")

    }
  }

  object local {

    var distFolder: String = new File(".").getAbsolutePath

    def load(props: Properties): Unit = {

      if (props.containsKey("local.distFolder")) distFolder = props.getProperty("local.distFolder")

    }
  }

  def load(): Unit = {
    load(DEFAULT_FILE)
  }

  def validationCheck(): Unit = {
    if (!Array("yarn", "mesos").contains(mode)) {
      throw new ConfigurationException(s"mode $mode is not legal. Options are 'yarn' or 'mesos'!")
    }
  }

  def load(file: InputStream): Unit = {
    val props: Properties = new Properties()

    props.load(file)
    file.close()

    if (props.containsKey("version")) version = props.getProperty("version")
    if (props.containsKey("user")) user = props.getProperty("user")
    if (props.containsKey("zk")) zk = props.getProperty("zk")
    if (props.containsKey("master")) master = props.getProperty("master")
    if (props.containsKey("masterPort")) masterPort = props.getProperty("masterPort")
    if (props.containsKey("timeout")) timeout = props.getProperty("timeout").asInstanceOf[Double]
    if (props.containsKey("mode")) mode = props.getProperty("mode")
    if (props.containsKey("workingFolder")) workingFolder = props.getProperty("workingFolder", s"/user/$user")
    if (props.containsKey("pysparkPath")) pysparkPath = props.getProperty("pysparkPath") else pysparkPath = s"spark-${props.getProperty("spark.version")}/bin/spark-submit"
    // TODO: rethink this
    Jar = this.getClass.getProtectionDomain.getCodeSource.getLocation.toURI.getPath
    JarName = Paths.get(this.getClass.getProtectionDomain.getCodeSource.getLocation.getPath).getFileName.toString

    Jobs.load(props)
    Webserver.load(props)
    YARN.load(props)
    spark.load(props)

    distLocation match {

      case "AWS" => AWS.load(props)
      case "local" => local.load(props)
      case _ => log.error("The distribution location must be a valid file system: local, HDFS, or AWS for S3")

    }
    AWS.load(props)

    validationCheck()
  }

}

object ClusterConfig {

  def apply(file: InputStream): ClusterConfig = {

    val config = new ClusterConfig()
    config.load(file)

    config
  }

  def apply(): ClusterConfig = {
    val config = new ClusterConfig()
    config.load()
    config
  }

}