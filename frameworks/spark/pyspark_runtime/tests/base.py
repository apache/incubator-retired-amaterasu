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
import shutil
from unittest import mock, TestCase
from pyspark.sql import SparkSession

env = mock.MagicMock()
notifier = mock.MagicMock()


class BaseSparkUnitTest(TestCase):

    job_name = 'test'
    spark: SparkSession = None
    WORK_DIR = '/tmp/amaterasu'

    @staticmethod
    def get_spark_session(job_name) -> SparkSession:
        return (SparkSession.builder
                .master('local[*]')
                .appName(job_name)
                .getOrCreate())

    @classmethod
    def setUpClass(cls):
        cls.spark = cls.get_spark_session(cls.job_name)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        if os.path.exists(cls.WORK_DIR):
            shutil.rmtree(cls.WORK_DIR)

    def tearDown(self):
        if os.path.exists(self.WORK_DIR):
            shutil.rmtree(self.WORK_DIR)
