import py4j
from py4j.java_gateway import JavaGateway
gateway = JavaGateway()
queue = gateway.jvm.io.shinto.amaterasu.execution.actions.runners.spark.PySparkExecutionQueue

while True :
  line = queue.getNext()
