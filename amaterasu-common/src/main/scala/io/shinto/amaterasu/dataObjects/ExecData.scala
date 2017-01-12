package io.shinto.amaterasu.dataObjects

import io.shinto.amaterasu.execution.dependencies.Dependencies
import io.shinto.amaterasu.runtime.Environment

case class ExecData(env: Environment, deps: Dependencies)
