package org.apache.amaterasu.leader.utilities

object MemoryFormatParser {

  def extractMegabytes(input: String): Int = {
    var result: Int = 0
    val lower = input.toLowerCase
    if (lower.contains("mb")) {
      result = lower.replace("mb", "").toInt
    } else if (lower.contains("gb") | lower.contains("g")) {
      result = lower.replace("g", "").replace("b","").toInt * 1024
    } else {
      result = lower.toInt
    }

    result
  }
}
