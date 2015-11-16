package io.shinto.amaterasu.utilities

import java.io.{ FileNotFoundException, File }

import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.PutObjectRequest
import io.shinto.amaterasu.{ Logging, Config }
import org.apache.commons.io.FileUtils

/**
  * A utility for interacting with the underlying file system
  * currently support Local, HDFS and S3
  */
class FsUtil extends Logging {

  var config: Config = null

  private def distToS3(): Unit = {

    val awsCreds = new BasicAWSCredentials(config.AWS.accessKeyId, config.AWS.secretAccessKey)
    val s3Client = new AmazonS3Client(awsCreds)

    val file = new File(config.JobSchedulerJar)

    s3Client.putObject(new PutObjectRequest(
      config.AWS.distBucket, config.AWS.distFolder, file
    ))

  }

  private def distToLocal(): Unit = {

    FileUtils.copyFile(
      new File(config.JobSchedulerJar),
      new File(s"${config.local.distFolder}/${config.JobSchedulerJar}")
    )

  }

  def distributeJar(): Unit = {

    config.distLocation match {

      case "AWS"   => distToS3()
      case "local" => distToLocal()
      case _       => log.error("The distribution location must be a valid file system: local, HDFS, or AWS for S3")

    }

  }

  private def getS3Jar(): String = {

    val awsCreds = new BasicAWSCredentials(config.AWS.accessKeyId, config.AWS.secretAccessKey)
    val s3Client = new AmazonS3Client(awsCreds)

    s3Client.getUrl(config.AWS.distBucket, config.AWS.distFolder).toString

  }

  private def getLocalJar(): String = {

    new File(config.JobSchedulerJar).toURI.toURL.toString.replace("file:/", "file:///")

  }

  def getJarUrl(): String = {

    config.distLocation match {

      case "AWS"   => getS3Jar()
      case "local" => getLocalJar()
      case _       => throw new FileNotFoundException()

    }
  }
}

object FsUtil {

  def apply(config: Config): FsUtil = {

    val result = new FsUtil
    result.config = config
    result

  }

}

