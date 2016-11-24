#!/usr/bin/python

import py4j
import ast
import codegen
import sys

from pyspark.rdd import RDD
from pyspark.files import SparkFiles
from pyspark.storagelevel import StorageLevel
from pyspark.accumulators import Accumulator, AccumulatorParam
from pyspark.broadcast import Broadcast
from pyspark.serializers import MarshalSerializer, PickleSerializer
from pyspark.sql import SchemaRDD
from py4j.java_gateway import JavaGateway,java_import, GatewayClient, GatewayParameters
from pyspark.conf import SparkConf
from pyspark.context import SparkContext

client = GatewayClient(port=int(sys.argv[1]))
gateway = JavaGateway(client,auto_convert = True)
entry_point = gateway.entry_point
queue = entry_point.getExecutionQueue()

java_import(gateway.jvm, "io.shinto.amaterasu.execution.actions.runners.spark.PySpark.*")
java_import(gateway.jvm, "io.shinto.amaterasu.runtime.*")

java_import(gateway.jvm, "org.apache.spark.*")
java_import(gateway.jvm, "org.apache.spark.SparkContext")
java_import(gateway.jvm, "org.apache.spark.api.java.*")
java_import(gateway.jvm, "org.apache.spark.sql.*")
java_import(gateway.jvm, "org.apache.spark.sql.hive.*")
java_import(gateway.jvm, "org.apache.spark.api.python.*")
java_import(gateway.jvm, "org.apache.spark.mllib.api.python.*")
java_import(gateway.jvm, "java.util.*")
java_import(gateway.jvm, "org.apache.spark.SparkEnv")
java_import(gateway.jvm, "org.apache.spark.SparkConf")


jsc = entry_point.getJavaSparkContext()
jconf = entry_point.getSparkConf()

java_import(gateway.jvm, "scala.Tuple2")

# this is an ugly hack
# this is an ugly hack
# this is an ugly hack...
# xsc = jsc.sc
# def sc():
#     return xsc
#
# setattr(jsc,"sc", sc)

# with open('/Users/roadan/2sc.txt', 'a') as the_file:
#     the_file.write(str(jsc.sc()) + '\n')

conf = SparkConf(_jvm = gateway.jvm, _jconf = jconf)
sc = SparkContext(jsc=jsc, gateway=gateway, conf=conf)

while True:
    actionData = queue.getNext()
    resultQueue = entry_point.getResultQueue(actionData._2())
    actionSource = actionData._1()

    tree = ast.parse(actionSource)

    for node in tree.body:

        wrapper = ast.Module(body=[node])
        try:
            co  = compile(wrapper, "<ast>", 'exec')
            exec(co)
            resultQueue.put('success', actionData._2(), codegen.to_source(node), '')

        except:
            resultQueue.put('error', actionData._2(), codegen.to_source(node), str(sys.exc_info()[1]))

    resultQueue.put('completion', '', '', '')