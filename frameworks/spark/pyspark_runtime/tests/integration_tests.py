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
from virtualenv import create_environment
from amaterasu import conf
from unittest import TestCase, mock
mock_stomp = mock.MagicMock()
mock_stomp.send = lambda x: print(x)
mock.patch('stomp.Connection', new_callable=mock_stomp)
from amaterasu_pyspark.runtime import ama_context
import subprocess
import sys
import shutil
import os

venv_dir = '/tmp/ama_test_dir/venv'
PYTHON_EXEC = '{}/bin/python'.format(venv_dir)

def _create_amaterasu_venv():
    create_environment(venv_dir)
    pip_path = '{}/bin/pip'.format(venv_dir)
    _run_program(pip_path, 'install', '-U', '../../../../sdk_python/')
    _run_program(pip_path, 'install', '-U', '../')


def setUpModule():
    _create_amaterasu_venv()

# def tearDownModule():
#     shutil.rmtree(venv_dir)


def _run_program(program, *args, **kwargs):
    extra_env = kwargs.get('env', {})
    env = os.environ.copy()
    env.update(extra_env)
    if sys.version_info.minor >= 5:
        subprocess.run((program, *args), env=env)
    else:
        subprocess.call((program, *args), env=env)


class ScriptsWithSparkSubmit(TestCase):

    def test_simple_script_with_spark_submit_should_persist_list_of_squares(self):
        spark_submit = '{}/bin/spark-submit'
        _run_program('spark-submit', 'test_scripts/simple.py', env={
            'PYSPARK_PYTHON': PYTHON_EXEC,
        })
        sdf = ama_context.get_dataset(conf.job_metadata.actionName, 'odd')
        stored_list = sdf.select('pow_number').orderBy('pow_number').rdd.flatMap(lambda x: x).collect()
        expected_list = [1, 4, 9, 16]
        self.assertEqual(stored_list, expected_list)




class ActualScriptsTests(TestCase):


    def tearDown(self):
        shutil.rmtree(conf.env.workingDir.split('file://')[1])

    def test_simple_script(self):
        _run_program(PYTHON_EXEC, 'test_scripts/simple.py')
        sdf = ama_context.get_dataset(conf.job_metadata.actionName, 'odd')
        stored_list = sdf.select('pow_number').orderBy('pow_number').rdd.flatMap(lambda x: x).collect()
        expected_list = [1, 4, 9, 16]
        self.assertEquals(stored_list, expected_list)

