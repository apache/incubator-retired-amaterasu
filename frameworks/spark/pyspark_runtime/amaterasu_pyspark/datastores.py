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

