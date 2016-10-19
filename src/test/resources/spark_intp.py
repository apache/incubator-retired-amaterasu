#!/usr/bin/python

import py4j
import ast

from py4j.java_gateway import JavaGateway,java_import

gateway = JavaGateway()
entry_point = gateway.entry_point
queue = entry_point.getExecutionQueue()

java_import(gateway.jvm, "scala.Tuple2")

while True:
  actionData = queue.getNext()
  resultQueue = entry_point.getResultQueue(actionData._2())
  actionSource = actionData._1()

  tree = ast.parse(actionSource)
  with open('/Users/roadan/out_source.txt', 'a') as the_file:
    the_file.write(ast.dump(tree))


