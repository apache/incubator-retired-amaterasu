<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one or more
  ~ contributor license agreements.  See the NOTICE file distributed with
  ~ this work for additional information regarding copyright ownership.
  ~ The ASF licenses this file to You under the Apache License, Version 2.0
  ~ (the "License"); you may not use this file except in compliance with
  ~ the License.  You may obtain a copy of the License at
  ~
  ~      http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing, software
  ~ distributed under the License is distributed on an "AS IS" BASIS,
  ~ WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  ~ See the License for the specific language governing permissions and
  ~ limitations under the License.
  -->  
##Overview

One of the core capabilities of Apache Amaterasu is configuration management for data pipelines. Configurations are stored in environments. By default, environments are defined in folders named `Env` that can be stored both at the root of the Amaterasu repo which is applied to all the actions in the repo as well as in the action folder under: `src/{action_name}/{env}/` which are available only for the specific action. 

**Note:** When the same configuration value is defined at the root and for an action, the action level definition overrides the the global configuration.

The following repo structure defines three environments (`dev`, `test` and `prod`) both at the root and for the `start` action:
 
```
repo
+-- env/
|   +-- dev/
|   |   +-- job.yaml
|   |   +-- spark.yaml
|   |   +-- datasets.yaml
|   +-- test/
|   |   +-- job.yaml
|   |   +-- spark.yaml
|   |   +-- datasets.yaml
|   +-- prod/
|       +-- job.yaml
|       +-- spark.yaml
|       +-- datasets.yaml
+-- src/
|   +-- start/
|       +-- dev/
|       |   +-- job.yaml
|       |   +-- spark.yaml
|       +-- test/
|       |   +-- job.yaml
|       |   +-- spark.yaml
|       +-- prod/
|           +-- job.yaml
|           +-- spark.yaml
+-- maki.yaml 

```

## Custom configuration locations

Additional configuration paths can be added both for global and action configurations by specifying the `config` element in the `maki.yaml` as shown in the following example:

```yaml
config: myconfig/{env}/
job-name:    amaterasu-test
flow:
    - name: start
      config: cfg/start/{env}/
      runner:
          group: spark
          type: python        
      file: start.py

```

## Configuration Types

Amaterasu allows the configuration of three main areas:

### Frameworks

All frameworks have their own configuration, Apache Amaterasu allows different frameworks to define their configurations per environment and by doing so, allowing to configure how actions will be configured when deployed.

For more information about specific framework configuration options, look at the [frameworks](frameworks/) section of this documentation.

### Datasets 

One of the main concerns when running a data-pipelines is where and how you get the data required to run the jobs.

Amaterasu provides a mechanism for managing and configuring such data-sources. Each one of our SDKs provides a way to load and persist data easily. This functionality is based on prior configuration.

In a job repository, each environment contains a ```datasets.yml``` file. This file contains the configurations of all datasets to be used in the job.

Below is an example of a simple configuration, for a dataset stored as parquet in Amazon S3.

```yaml
file:
  - uri: s3a://amaterasu-example/input/random-beers
    format: parquet
    name: random-beers
```

#### Detailed configuration
Amaterasu supports different types of datasets and their corresponding configuration options.
Do note that different Apache Amaterasu frameworks may have their take on the configurations below.
##### File
Currently, the following formats are supported - JSON, parquet, CSV, ORC.

The following storage types are currently supported - s3, azure blob storage, file system, HDFS.

The following configuration options are currently supported:

| Parameter | Description |
|:---------:|:-----------:|
| name      |The name of the dataset|
| uri       |The file URI. Supported schemes are: s3a, file, hdfs, wasb, wasbs, gs|
| format    |The file format - JSON, parquet, CSV, ORC|

Example:
```yaml
file:
  - uri: s3a://amaterasu-example/input/random-beers
    format: parquet
    name: random-beers
``` 

> Note! If the scheme or format isn't currently supported by the available Apache Amaterasu frameworks, it is still possible to define using a [generic dataset](#generic-datasets).
 
 ##### Hive Table
 
 Example:
 ```yaml
hive:
    - table: mytable
      name: mydataset 

```

##### Generic Datasets
Aside from hive tables and files, there is a need to allow configuration of other data sources, even if they aren't currently supported by Apache Amaterasu. 

There is a wide set of use cases -
 * Pulling data from external APIs, where API keys change between environments (prod and dev keys)
 * Pulling data from organizational web services
 * Pulling data from relational databases
 * and many more.
 
 To support the configuration management for such use cases, Apache Amaterasu provides the means to define generic datasets.
 
 Example:
 ```yaml
 generic:
    - name: mygenericds 
      key1: value1
      key2: value2
      
    - name: myothergenericds
      key1: value1
      key2: value2
 ``` 

The usage of generic datasets is explained in the relevant SDK documentation [section](./frameworks.md#integration-with-unsupported-data-sources)

### Custom Configuration