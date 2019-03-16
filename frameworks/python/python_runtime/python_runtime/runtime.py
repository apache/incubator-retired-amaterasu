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
from amaterasu import BaseAmaContext
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, DataFrame

from amaterasu.datasets import BaseDatasetManager
from amaterasu.runtime import AmaContextBuilder
from .datasets import DatasetManager
import os


class PythonAmaContextBuilder(AmaContextBuilder):

    def build(self) -> "AmaContext":
        return AmaContext(self.ama_conf)


class AmaContext(BaseAmaContext):

    @classmethod
    def builder(cls) -> AmaContextBuilder:
        return PythonAmaContextBuilder()

    @property
    def dataset_manager(self) -> BaseDatasetManager:
        return self._dataset_manager

    def __init__(self, ama_conf):
        super(AmaContext, self).__init__(ama_conf)
        self._dataset_manager = DatasetManager(ama_conf.datasets)

    def get_dataset(self, dataset_name: str) -> DataFrame:
        return self._dataset_manager.load_dataset(dataset_name)

    def persist(self, dataset_name: str, dataset: DataFrame, overwrite: bool = True):
        self._dataset_manager.persist_dataset(dataset_name, dataset, overwrite)


