from amaterasu import conf, notifier, ImproperlyConfiguredError, BaseAmaContext
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, DataFrame


class AmaContext(BaseAmaContext):

    def __init__(self, sc=None, spark=None):
        super(AmaContext, self).__init__()
        if sc and spark:
            self.sc = sc
            self.spark = spark
        else:
            try:
                master = conf.env.master
            except AttributeError:
                raise ImproperlyConfiguredError("No SPARK_MASTER environment variable was defined!")
            else:
                spark_conf = SparkConf().setAppName(conf.env.name).setMaster(master)
                self.sc = SparkContext.getOrCreate(spark_conf)
                self.spark = SparkSession(self.sc)

    def get_dataset(self, action_name, dataset_name, format="parquet"):
        return self.spark.read.format(format).load(str(
            conf.env.workingDir) + "/" + conf.job_metadata.jobId + "/" + action_name + "/" + dataset_name)

    def persist(self, dataset_name, dataset, format='parquet', overwrite=True):
        """
        Persists a dataset to the chosen storage backend.
        :param action_name: The name as described in the maki.yaml
        :param dataset_name: The requested export name
        :param dataset: The PySpark Dataset itself.
        :param format: JSON, Parquet, etc.
        :type action_name: str
        :type dataset_name: str
        :type dataset: DataFrame
        :type format: str
        :return:
        """
        if dataset_name in conf.exports:
            format = conf.exports[dataset_name]

        writer = dataset.write.format(format)
        if overwrite:
            writer = writer.mode('overwrite')
        writer.save(
            conf.env.workingDir + "/" + conf.job_metadata.jobId + "/" + conf.job_metadata.actionName + "/" + dataset_name)


try:
    ama_context = AmaContext(sc, spark)
except NameError:  # Mainly for testing, but also to run outside of spark-submit.
    ama_context = AmaContext()