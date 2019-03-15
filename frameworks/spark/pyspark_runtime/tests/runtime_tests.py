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
import unittest

from pyspark.sql import DataFrame

from amaterasu_pyspark.runtime import AmaContext
from amaterasu.datasets import DatasetNotFoundError, DatasetTypeNotSupported


class DatastoresTests(unittest.TestCase):

    def setUp(self):
        self.ama_context = AmaContext.builder().build()

    def test_loading_an_existing_generic_dataset_should_not_be_implemented(self):
        self.assertRaises(NotImplementedError,  self.ama_context.get_dataset, "mydataset")

    def test_loading_an_unsupported_dataset_should_raise_an_exception(self):
        self.assertRaises(DatasetTypeNotSupported, self.ama_context.get_dataset, "unsupported")

    def test_loading_a_dataset_that_is_not_defined_should_raise_an_exception(self):
        self.assertRaises(DatasetNotFoundError, self.ama_context.get_dataset, "notfound")

    def test_load_dataset_from_file_should_return_a_dataframe(self):
        df = self.ama_context.get_dataset('input_file')
        self.assertEquals(type(df), DataFrame)