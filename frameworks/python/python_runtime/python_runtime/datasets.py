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
import json
import csv
import os
from abc import ABC
from typing import Type, Dict, Any

from amaterasu.datasets import BaseDatasetLoader, BaseDatasetManager, DatasetTypes



class HiveDatasetLoader(BaseDatasetLoader):

    def load_dataset(self) -> DataFrame:
        return self.spark.sql("SELECT * FROM {}".format(self.dataset_conf['table']))

    def persist_dataset(self, dataset: DataFrame, overwrite: bool = False):
        if overwrite:
            self.spark.sql('DROP TABLE {}'.format(self.dataset_conf['table']))
        dataset.write.saveAsTable(self.dataset_conf['table'])


class FileDatasetLoader(BaseDatasetLoader, ABC):

    registered_loaders = {}

    class LoaderRegisterer:

        def __init__(self, file_format):
            self.file_format = file_format

        def __call__(self, cls, *args, **kwargs):
            FileDatasetLoader.registered_loaders[self.file_format] = cls
            return cls

    register_loader = LoaderRegisterer

    def __new__(cls, dataset_conf: Dict, *args, **kwargs) -> BaseDatasetLoader:
        return cls.registered_loaders[dataset_conf['format']](dataset_conf, *args, **kwargs)


@FileDatasetLoader.register_loader("json")
class JSONFileDatasetLoader(BaseDatasetLoader):
    def load_dataset(self) -> Any:
        return json.load(self.dataset_conf['uri'])

    def persist_dataset(self, dataset: Any, overwrite: bool):
        if not overwrite and os.path.exists(self.dataset_conf['uri']):
            raise IOError("File '{}' already exists".format(self.dataset_conf['uri']))
        else:
            json.dump(dataset, self.dataset_conf['uri'])


@FileDatasetLoader.register_loader("csv")
class CSVFileDatasetLoader(BaseDatasetLoader):

    def load_dataset(self) -> Any:
        with open(self.dataset_conf['uri']) as f:
            return list(csv.DictReader(f))

    def persist_dataset(self, dataset: Any, overwrite: bool):
        pass


class DatasetManager(BaseDatasetManager):

    def get_datastore(self, datastore_cls: Type[BaseDatasetLoader], dataset_conf: Dict):
        datastore = datastore_cls(dataset_conf)
        return datastore

    def __init__(self, dataset_conf):
        super(DatasetManager, self).__init__(dataset_conf)
        self._registered_datastores[DatasetTypes.Hive.value] = HiveDatasetLoader
        self._registered_datastores[DatasetTypes.File.value] = FileDatasetLoader

