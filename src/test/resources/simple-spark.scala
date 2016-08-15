import io.shinto.amaterasu.execution.AmaContext
import org.apache.spark.sql.{SaveMode, DataFrame}

val data = Array(1, 2, 3, 4, 5)

val sc = AmaContext.sc
val rdd = sc.parallelize(data)
val sqlContext = AmaContext.sqlContext

import sqlContext.implicits._
val x: DataFrame = rdd.toDF()

x.write.mode(SaveMode.Overwrite)