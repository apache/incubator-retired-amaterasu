from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, IntegerType

from amaterasu.pyspark.runtime import ama_context


def pow(num):
    return num * num


pow_udf = udf(pow, IntegerType())
a = [[1], [2], [3], [4]]
schema = StructType([
    StructField('number', IntegerType(), True)
])
input_df = ama_context.spark.createDataFrame(a, schema)
sdf = input_df.withColumn("pow_number", pow_udf("number"))

ama_context.persist('odd', sdf, overwrite=True)
