"""
 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
"""
from abc import ABC
from pyspark.sql import SparkSession, DataFrame
from typing import Type, Dict

from amaterasu.base import BaseDatasetLoader, BaseDatasetManager, DatasetTypes


class BaseSparkDatasetLoader(BaseDatasetLoader, ABC):

    def __init__(self, dataset_conf: Dict, spark: SparkSession):
        super(BaseSparkDatasetLoader, self).__init__(dataset_conf)
        self.spark = spark


class HiveDatasetLoader(BaseSparkDatasetLoader):

    def load_dataset(self) -> DataFrame:
        return self.spark.sql("SELECT * FROM {}".format(self.dataset_conf['table']))

    def persist_dataset(self, dataset: DataFrame, overwrite: bool = False):
        if overwrite:
            self.spark.sql('DROP TABLE {}'.format(self.dataset_conf['table']))
        dataset.write.saveAsTable(self.dataset_conf['table'])


class FileDatasetLoader(BaseSparkDatasetLoader):

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
                .save(self.dataset_conf['uri'])
        else:
            dataset.write\
                .format(self.dataset_conf['format']) \
                .save(self.dataset_conf['uri'])


class DatasetManager(BaseDatasetManager):

    def get_datastore(self, datastore_cls: Type[BaseSparkDatasetLoader], dataset_conf: Dict):
        datastore = datastore_cls(dataset_conf, self.spark)
        return datastore

    def __init__(self, dataset_conf, spark):
        super(DatasetManager, self).__init__(dataset_conf)
        self.spark = spark
        self._registered_datastores[DatasetTypes.Hive.value] = HiveDatasetLoader
        self._registered_datastores[DatasetTypes.File.value] = FileDatasetLoader

