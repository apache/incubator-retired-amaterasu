#!/usr/bin/python

import py4j
import ast
import codegen
import sys

from py4j.java_gateway import JavaGateway,java_import

gateway = JavaGateway()
entry_point = gateway.entry_point
queue = entry_point.getExecutionQueue()

java_import(gateway.jvm, "scala.Tuple2")

java_import(gateway.jvm, "io.shinto.amaterasu.execution.actions.runners.spark.PySpark.*")

java_import(gateway.jvm, "org.apache.spark.SparkEnv")
java_import(gateway.jvm, "org.apache.spark.SparkConf")
java_import(gateway.jvm, "org.apache.spark.api.java.*")
java_import(gateway.jvm, "org.apache.spark.sql.*")
java_import(gateway.jvm, "org.apache.spark.sql.hive.*")
java_import(gateway.jvm, "org.apache.spark.api.python.*")
java_import(gateway.jvm, "org.apache.spark.mllib.api.python.*")

sc = entry_point.getSparkContext()

while True:
  actionData = queue.getNext()
  resultQueue = entry_point.getResultQueue(actionData._2())
  actionSource = actionData._1()

  tree = ast.parse(actionSource)

  str1 = ''
  for node in tree.body:
    wrapper = ast.Module(body=[node])
    try:
      co = compile(wrapper, "<ast>", 'exec')
      exec(co)
      resultQueue.put('success', actionData._2(), codegen.to_source(node), '')
    except:
      resultQueue.put('error', actionData._2(), codegen.to_source(node), str(sys.exc_info()[0]))

  resultQueue.put('completion', '', '', '')

