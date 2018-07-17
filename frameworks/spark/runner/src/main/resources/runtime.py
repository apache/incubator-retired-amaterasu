#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
class AmaContext(object):

    def __init__(self, sc, spark, job_id, env):
        self.sc = sc
        self.spark = spark
        self.job_id = job_id
        self.env = env

    def get_dataframe(self, action_name, dataset_name, format = "parquet"):
        return self.spark.read.format(format).load(str(self.env.working_dir) + "/" + self.job_id + "/" + action_name + "/" + dataset_name)

class Environment(object):

    def __init__(self, name, master, input_root_path, output_root_path, working_dir, configuration):
        self.name = name
        self.master = master
        self.input_root_path = input_root_path
        self.output_root_path = output_root_path
        self.working_dir = working_dir
        self.configuration = configuration
