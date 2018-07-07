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
import stat
import errno
from amaterasu.cli.compat import *
from amaterasu.cli.common import Resources

def handleRemoveReadonly(func, path, exc):
    excvalue = exc[1]
    if func in (os.rmdir, os.remove, os.unlink) and excvalue.errno == errno.EACCES:
        os.chmod(path, stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)  # 0777
        func(path)
    else:
        raise exc[0]


def before_all(context):
    context.test_resources = Resources(os.path.join(os.getcwd(), 'tests'))
    context.first_run = False  # overridden in run_pipeline_unit.feature::It is the first time the user runs a pipeline


def before_scenario(context, scenario):
    try:
        shutil.rmtree("/tmp/amaterasu-repos", onerror=handleRemoveReadonly)
    except (FileNotFoundError, WindowsError):
        pass
    context.stats_before = {}
    context.stats_after = {}
    try:
        shutil.rmtree(os.path.abspath('tmp'), onerror=handleRemoveReadonly)
    except (FileNotFoundError, WindowsError):
        pass
    os.mkdir(os.path.abspath('tmp'))


def after_scenario(context, scenario):
    shutil.rmtree(os.path.abspath('tmp'), onerror=handleRemoveReadonly)