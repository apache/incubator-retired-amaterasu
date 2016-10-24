#!/usr/bin/python

import py4j
import ast
import codegen

from py4j.java_gateway import JavaGateway,java_import

gateway = JavaGateway()
entry_point = gateway.entry_point
queue = entry_point.getExecutionQueue()

java_import(gateway.jvm, "scala.Tuple2")
java_import(gateway.jvm, "io.shinto.amaterasu.execution.actions.runners.spark.PySpark")

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
      str1 = str1 + '--->' + codegen.to_source(node) + '\n'
      #str1 = str1 + ast.dump(node) + '\n'
      resultQueue.put('success', actionData._2(), codegen.to_source(node), '')
    except Error:
      str1 = str1  + "Assertion failed on line" + node.lineno + ":" + '\n\n'
      if e.args:
        str1 + e +'\n\n'

  resultQueue.put('completion', '', '', '')

  with open('/Users/roadan/out_source.txt', 'a') as the_file:
    the_file.write(str1)
