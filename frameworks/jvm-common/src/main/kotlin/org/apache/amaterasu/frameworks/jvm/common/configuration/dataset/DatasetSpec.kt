package org.apache.amaterasu.frameworks.jvm.common.configuration.dataset

import com.uchuhimo.konf.ConfigSpec
import com.uchuhimo.konf.OptionalItem

class DataSetsSpec(configurationItem: String) {
    val spec = ConfigSpec()
    val items = OptionalItem(spec, configurationItem, emptyMap<String, String>())
}
