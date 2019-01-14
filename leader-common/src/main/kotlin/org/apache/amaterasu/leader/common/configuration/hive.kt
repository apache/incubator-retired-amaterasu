package org.apache.amaterasu.leader.common.configuration

import com.uchuhimo.konf.ConfigSpec

object hive : ConfigSpec("hive"){
    val datsets by Job.optional(emptyList<String, hiveDatasets>())
}

object hiveDatasets: ConfigSpec("hive.hiveDatasets"){
    val name by Job.required<String>()

}