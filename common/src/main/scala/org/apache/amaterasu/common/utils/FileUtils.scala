package org.apache.amaterasu.common.utils

import java.io.File

object FileUtils {

  def getAllFiles(dir: File): Array[File] = {
    val these = dir.listFiles
    these ++ these.filter(_.isDirectory).flatMap(getAllFiles)
  }

}
