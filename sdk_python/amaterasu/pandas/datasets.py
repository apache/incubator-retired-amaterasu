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
import os

import pandas as pd
from pyhive import hive
from typing import Type, Dict, Any

from amaterasu.base.datasets import BaseDatasetLoader, BaseDatasetManager, DatasetTypes


class HiveDatasetLoader(BaseDatasetLoader):

    def load_dataset(self) -> pd.DataFrame:
        hive_db = hive.connect(host=self.dataset_conf['host'], port=self.dataset_conf.get('port', 10000))
        with hive_db.cursor() as cursor:
            cursor.execute("SELECT * FROM {}".format(self.dataset_conf['table']))
            data = list(cursor.fetchall())
            return pd.DataFrame.from_records(data)

    def persist_dataset(self, dataset: pd.DataFrame, overwrite: bool = False, keep_index=False):
        hive_db = hive.connect(host=self.dataset_conf['host'], port=self.dataset_conf.get('port', 10000))
        with hive_db.cursor() as cursor:
            if overwrite:
                cursor.execute('DROP TABLE {}'.format(self.dataset_conf['table']))
            dataset.to_records(index=keep_index)
            dataset.write.saveAsTable(self.dataset_conf['table'])


class _FileCSVDatasetLoader(BaseDatasetLoader):

    def load_dataset(self, *args, **kwargs) -> pd.DataFrame:
        separator = self.dataset_conf.get('separator', ',')
        return pd.read_csv(self.dataset_conf['uri'], sep=separator)

    def persist_dataset(self, dataset: pd.DataFrame, overwrite: bool):
        separator = self.dataset_conf.get('separator', ',')
        dataset.to_csv(self.dataset_conf['conf'], sep=separator)


class _FileJSONDatasetLoader(BaseDatasetLoader):

    def load_dataset(self, *args, **kwargs) -> pd.DataFrame:
        orient = self.dataset_conf.get('orient')
        return pd.read_json(self.dataset_conf['uri'], orient=orient)

    def persist_dataset(self, dataset: pd.DataFrame, overwrite: bool):
        orient = self.dataset_conf.get('orient')
        dataset.to_json(self.dataset_conf['uri'], orient=orient)


class _FileExcelDatasetLoader(BaseDatasetLoader):

    def load_dataset(self, *args, **kwargs) -> pd.DataFrame:
        worksheet = self.dataset_conf['worksheet']
        return pd.read_excel(self.dataset_conf['uri'], sheet_name=worksheet)

    def persist_dataset(self, dataset: pd.DataFrame, overwrite: bool):
        worksheet = self.dataset_conf['worksheet']
        dataset.to_excel(self.dataset_conf['uri'], sheet_name=worksheet)


class _FileParquetDatasetLoader(BaseDatasetLoader):

    def load_dataset(self, *args, **kwargs) -> pd.DataFrame:
        return pd.read_parquet(self.dataset_conf['uri'])

    def persist_dataset(self, dataset: pd.DataFrame, overwrite: bool):
        dataset.to_parquet(self.dataset_conf['uri'])


class _FilePickleDatasetLoader(BaseDatasetLoader):

    def load_dataset(self, *args, **kwargs) -> pd.DataFrame:
        return pd.read_pickle(self.dataset_conf['uri'])

    def persist_dataset(self, dataset: pd.DataFrame, overwrite: bool):
        dataset.to_pickle(self.dataset_conf['uri'])


class FileDatasetLoader(BaseDatasetLoader):

    _registered_loaders : Dict[str, Type[BaseDatasetLoader]] = {
        'json': _FileJSONDatasetLoader,
        'excel': _FileExcelDatasetLoader,
        'csv': _FileCSVDatasetLoader,
        'pickle': _FilePickleDatasetLoader,
        'parquet': _FileParquetDatasetLoader
    }

    def __init__(self, dataset_conf: Dict):
        super().__init__(dataset_conf)
        try:
            self._concrete_loader = self._registered_loaders[self.dataset_conf['format']](self.dataset_conf)
        except KeyError:
            raise NotImplemented(
                "File with format '{}' is not supported, please handle it manually".format(self.dataset_conf['format']))

    def load_dataset(self) -> pd.DataFrame:
        return self._concrete_loader.load_dataset()

    def persist_dataset(self, dataset: pd.DataFrame, overwrite: bool):
        if not overwrite and os.path.exists(self.dataset_conf['uri']):
            raise IOError("File with path '{}' already exists.".format(self.dataset_conf['uri']))
        else:
            self._concrete_loader.persist_dataset(dataset, overwrite)


class DatasetManager(BaseDatasetManager):

    def get_datastore(self, datastore_cls: Type[BaseDatasetLoader], dataset_conf: Dict):
        datastore = datastore_cls(dataset_conf)
        return datastore

    def __init__(self, dataset_conf, spark):
        super(DatasetManager, self).__init__(dataset_conf)
        self.spark = spark
        self._registered_datastores[DatasetTypes.Hive.value] = HiveDatasetLoader
        self._registered_datastores[DatasetTypes.File.value] = FileDatasetLoader

