package io.shinto.amaterasu.common.dataobjects

import io.shinto.amaterasu.common.execution.dependencies.Dependencies
import io.shinto.amaterasu.common.runtime.Environment

case class ExecData(env: Environment, deps: Dependencies)
