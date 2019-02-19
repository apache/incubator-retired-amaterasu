from typing import Tuple

from amaterasu import conf, notifier, ImproperlyConfiguredError, BaseAmaContext
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, DataFrame

from amaterasu.datastores import BaseDatasetManager
from .datastores import DatasetManager


def _get_or_create_spark_attributes(sc: SparkContext, spark: SparkSession) -> Tuple[SparkContext, SparkSession]:
    if not sc or not spark:
        try:
            master = conf.env.master
        except AttributeError:
            raise ImproperlyConfiguredError("No SPARK_MASTER environment variable was defined!")
        else:
            spark_conf = SparkConf().setAppName(conf.env.name).setMaster(master)
            sc = SparkContext.getOrCreate(spark_conf)
            spark = SparkSession(sc)
    return sc, spark


class AmaContext(BaseAmaContext):

    @property
    def dataset_manager(self) -> BaseDatasetManager:
        return self._dataset_manager

    def __init__(self, sc: SparkContext = None, spark: SparkSession = None):
        super(AmaContext, self).__init__()
        self.sc, self.spark = _get_or_create_spark_attributes(sc, spark)
        self._dataset_manager = DatasetManager(self.spark)

    def get_dataset(self, dataset_name: str) -> DataFrame:
        return self._dataset_manager.load_dataset(dataset_name)

    def persist(self, dataset_name: str, dataset: DataFrame, overwrite: bool = True):
        self._dataset_manager.persist_dataset(dataset_name, dataset, overwrite)


try:
    ama_context = AmaContext(sc, spark)  # When using spark-submit
except NameError:
    ama_context = AmaContext()