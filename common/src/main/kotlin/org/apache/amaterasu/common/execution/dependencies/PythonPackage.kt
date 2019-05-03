package org.apache.amaterasu.common.execution.dependencies

data class PythonPackage(val packageId: String, val index: String? = null, val channel: String? = null)