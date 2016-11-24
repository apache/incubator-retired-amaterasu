//package io.shinto.amaterasu.utilities
//
//import java.io.{ FileNotFoundException, File }
//
//import com.amazonaws.auth.BasicAWSCredentials
//import com.amazonaws.services.s3.AmazonS3Client
//import com.amazonaws.services.s3.model.PutObjectRequest
//import io.shinto.amaterasu.Logging
//import io.shinto.amaterasu.configuration.{ ClusterConfig }
//import org.apache.commons.io.FileUtils
//
///**
//  * A utility for interacting with the underlying file system
//  * currently support Local, HDFS and S3
//  */
//class FsUtil extends Logging {
//
//  var config: ClusterConfig = null
//
//  private def distToS3(): Unit = {
//
//    val awsCreds = new BasicAWSCredentials(config.AWS.accessKeyId, config.AWS.secretAccessKey)
//    val s3Client = new AmazonS3Client(awsCreds)
//
//    val file = new File(config.Jar)
//
//    s3Client.putObject(new PutObjectRequest(
//      config.AWS.distBucket, config.AWS.distFolder, file
//    ))
//
//  }
//
//  private def distToLocal(): Unit = {
//
//    FileUtils.copyFile(
//      new File(config.Jar),
//      new File(s"${config.local.distFolder}/${config.Jar}")
//    )
//
//  }
//
//  def distributeJar(): Unit = {
//
//    config.distLocation match {
//
//      case "AWS"   => distToS3()
//      case "local" => distToLocal()
//      case _       => log.error("The distribution location must be a valid file system: local, HDFS, or AWS for S3")
//
//    }
//
//  }
//
//}
//
//object FsUtil {
//
//  def apply(config: ClusterConfig): FsUtil = {
//
//    val result = new FsUtil
//    result.config = config
//    result
//
//  }
//
//}
//
