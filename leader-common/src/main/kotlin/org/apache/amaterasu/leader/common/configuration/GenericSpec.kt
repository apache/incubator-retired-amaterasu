package org.apache.amaterasu.leader.common.configuration

import com.uchuhimo.konf.ConfigSpec
import com.uchuhimo.konf.OptionalItem

class GenericSpec(configurationItem: String) {
    val spec = ConfigSpec()
    val items = OptionalItem(spec, configurationItem, emptyMap<String, String>())
}