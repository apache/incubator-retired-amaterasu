package io.shinto.amaterasu

import java.io.{ FileInputStream, File }
import java.util.Properties

class Config {

  val DEFAULT_FILE = new File("amaterasu.properties")

  var user: String = ""
  var zk: String = ""
  var master: String = "127.0.0.1"
  var timeout: Double = 600000
  var taskMem: Int = 128
  var distLocation: String = ""
  var workingFolder: String = ""

  //this should be a filesystem path that is reachable by all executors (HDFS, S3, local)

  object Jobs {

    var cpus: Double = 1
    var mem: Long = 1024
    var repoSize: Long = 1024

    def load(props: Properties): Unit = {

      if (props.containsKey("jobs.cpu")) cpus = props.getProperty("jobs.cpu").asInstanceOf[Double]
      if (props.containsKey("jobs.mem")) mem = props.getProperty("jobs.mem").asInstanceOf[Long]
      if (props.containsKey("jobs.repoSize")) repoSize = props.getProperty("jobs.repoSize").asInstanceOf[Long]

    }

  }

  def load(): Unit = {
    load(DEFAULT_FILE)
  }

  def load(file: File): Unit = {
    val props: Properties = new Properties()
    val stream: FileInputStream = new FileInputStream(file)

    props.load(stream)
    stream.close()

    if (props.containsKey("user")) user = props.getProperty("user")
    if (props.containsKey("zk")) zk = props.getProperty("zk")
    if (props.containsKey("master")) master = props.getProperty("master")
    if (props.containsKey("timeout")) timeout = props.getProperty("timeout").asInstanceOf[Double]
    if (props.containsKey("workingFolder")) {
      workingFolder = props.getProperty("workingFolder")
    }
    else {
      workingFolder = s"/user/$user"
    }

    Jobs.load(props)

  }

}

object Config {
  def apply(): Config = {
    val config = new Config()
    config.load()

    config
  }
}