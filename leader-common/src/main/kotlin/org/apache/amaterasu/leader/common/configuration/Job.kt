package org.apache.amaterasu.leader.common.configuration

import com.uchuhimo.konf.ConfigSpec

object Job : ConfigSpec() {
    val name by required<String>()
    val master by required<String>()
    val inputRootPath by required<String>()
    val outputRootPath by required<String>()
    val workingDir by required<String>()
    val configuration by optional(emptyMap<String, String>())
}