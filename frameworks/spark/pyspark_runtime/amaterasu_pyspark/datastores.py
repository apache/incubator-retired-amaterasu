from abc import ABC
from pyspark.sql import SparkSession, DataFrame
from typing import Type, Dict

from amaterasu.datastores import BaseDatastore, BaseDatasetManager, DatasetTypes


class BaseSparkStore(BaseDatastore, ABC):

    def __init__(self, dataset_conf: Dict, spark: SparkSession):
        super(BaseSparkStore, self).__init__(dataset_conf)
        self.spark = spark


class HiveStore(BaseSparkStore):

    def load_dataset(self) -> DataFrame:
        return self.spark.sql("SELECT * FROM {}".format(self.dataset_conf['table']))

    def persist_dataset(self, dataset: DataFrame, overwrite: bool = False):
        if overwrite:
            self.spark.sql('DROP TABLE {}'.format(self.dataset_conf['table']))
        dataset.write.saveAsTable(self.dataset_conf['table'])


class FileStore(BaseSparkStore):

    def load_dataset(self) -> DataFrame:
        return self.spark\
            .read\
            .format(self.dataset_conf['format'])\
            .load(self.dataset_conf['uri'])

    def persist_dataset(self, dataset: DataFrame, overwrite: bool):
        if overwrite:
            dataset.write \
                .mode('overwrite') \
                .format(self.dataset_conf['format']) \
                .load(self.dataset_conf['uri'])
        else:
            dataset.write\
                .format(self.dataset_conf['format']) \
                .load(self.dataset_conf['uri'])


class DatasetManager(BaseDatasetManager):

    def get_datastore(self, datastore_cls: Type[BaseSparkStore], dataset_conf: Dict):
        datastore = datastore_cls(dataset_conf, self.spark)
        return datastore

    def __init__(self, spark):
        super(DatasetManager, self).__init__()
        self.spark = spark
        self._registered_datastores[DatasetTypes.Hive.value] = HiveStore
        self._registered_datastores[DatasetTypes.File.value] = FileStore

