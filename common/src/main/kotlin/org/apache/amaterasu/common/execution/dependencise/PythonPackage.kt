package org.apache.amaterasu.common.execution.dependencise

data class PythonPackage(val packageId: String, val index: String? = null, val channel: String? = null)