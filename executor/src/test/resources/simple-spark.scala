
import org.apache.amaterasu.executor.runtime.AmaContext
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

val data = Seq(1,3,4,5,6)


val sc = AmaContext.sc
val rdd = sc.parallelize(data)
val sqlContext = AmaContext.spark

import sqlContext.implicits._
val x: DataFrame = rdd.toDF()

x.write.mode(SaveMode.Overwrite)