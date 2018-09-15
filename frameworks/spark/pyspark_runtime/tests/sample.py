from amaterasu_pyspark.runtime import ama_context, notifier

rdd = ama_context.sc.parallelize([1,2,3,4])

ama_context.persist('extraction', 'extracted_raw_data', rdd.toDF())
ama_context.get_dataset('extraction', 'extracted_raw_data')

notifier.info('')