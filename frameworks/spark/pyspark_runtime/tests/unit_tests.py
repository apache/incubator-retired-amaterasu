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
from unittest import mock
mock_stomp = mock.MagicMock()
mock_stomp.send = lambda x: print(x)
mock.patch('stomp.Connection', new_callable=mock_stomp)
from amaterasu_pyspark.runtime import ama_context, conf
from base import BaseSparkUnitTest
from pyspark.sql.types import StructType, StructField, IntegerType


class TestAmaSparkContextPersistence(BaseSparkUnitTest):

    def test_ama_context_persist_with_valid_df_and_path_should_be_stored_successfully(self):
        a = [[1],[2],[3],[4]]
        schema = StructType([
            StructField('number', IntegerType(), True)
        ])
        input_df = ama_context.spark.createDataFrame(a, schema)
        ama_context.persist('test_df', input_df)
        stored_df = self.spark.read.format('parquet').load(conf.env.workingDir + "/" + conf.job_metadata.jobId + "/" + conf.job_metadata.actionName + "/test_df")
        input_list = input_df.select('number').collect()
        stored_list = stored_df.select('number').orderBy('number').collect()
        self.assertEqual(input_list, stored_list)

    def test_ama_context_read_df_from_valid_path_should_yield_dataframe(self):
        a = [[1], [2], [3], [4]]
        schema = StructType([
            StructField('number', IntegerType(), True)
        ])
        input_df = ama_context.spark.createDataFrame(a, schema)
        input_df.write.format('parquet').mode('overwrite').save(conf.env.workingDir + "/" + conf.job_metadata.jobId + "/test/test_df")
        stored_df = ama_context.get_dataset('test', 'test_df')
        input_list = input_df.select('number').collect()
        stored_list = stored_df.select('number').orderBy('number').collect()
        self.assertEqual(input_list, stored_list)

    def test_ama_contrxt_write_then_read_should_yield_same_dataframe(self):
        a = [[1], [2], [3], [4]]
        schema = StructType([
            StructField('number', IntegerType(), True)
        ])
        input_df = ama_context.spark.createDataFrame(a, schema)
        ama_context.persist('odd', input_df)
        stored_df = ama_context.get_dataset(conf.job_metadata.actionName, 'odd')
        input_list = input_df.select('number').collect()
        stored_list = stored_df.select('number').orderBy('number').collect()
        self.assertEqual(input_list, stored_list)