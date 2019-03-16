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

spark_env_content = '''
#!/usr/bin/env bash

export PYSPARK_PYTHON={}
'''.format(os.getenv("PYSPARK_PYTHON"))
pyspark_env_path = '{}/conf/spark_env.sh'.format(os.getenv('SPARK_HOME'))

with open(pyspark_env_path, 'w') as f:
    f.write(spark_env_content)


class SparkAmaContextBuilder(AmaContextBuilder):

    def __init__(self):
        super().__init__()
        if self.ama_conf:
            self.spark_conf = SparkConf()\
                .setAppName('amaterasu-{}-{}'.format(self.ama_conf.runtime.jobId, self.ama_conf.runtime.actionName))\
                .setMaster(self.ama_conf.env.master)\
                .set("spark.pyspark.python", os.getenv("PYSPARK_PYTHON"))
        else:
            self.spark_conf = SparkConf()

    def setMaster(self, master_uri) -> "AmaContextBuilder":
        self.spark_conf.setMaster(master_uri)
        return self

    def set(self, key, value) -> "AmaContextBuilder":
        self.spark_conf.set(key, value)
        return self

    def build(self) -> "AmaContext":
        spark = SparkSession.builder.config(conf=self.spark_conf).getOrCreate()
        sc = spark.sparkContext
        return AmaContext(self.ama_conf, sc, spark)


class AmaContext(BaseAmaContext):

    @classmethod
    def builder(cls) -> AmaContextBuilder:
        return SparkAmaContextBuilder()

    @property
    def dataset_manager(self) -> BaseDatasetManager:
        return self._dataset_manager

    @property
    def sc(self) -> SparkContext:
        return self._sc

    @property
    def spark(self) -> SparkSession:
        return self._spark

    def __init__(self, ama_conf, sc: SparkContext = None, spark: SparkSession = None):
        super(AmaContext, self).__init__(ama_conf)
        self._sc, self._spark = sc, spark
        self._dataset_manager = DatasetManager(ama_conf.datasets, self.spark)

    def get_dataset(self, dataset_name: str) -> DataFrame:
        return self._dataset_manager.load_dataset(dataset_name)

    def persist(self, dataset_name: str, dataset: DataFrame, overwrite: bool = True):
        self._dataset_manager.persist_dataset(dataset_name, dataset, overwrite)


