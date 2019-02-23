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
|   +-- test/
|   |   +-- job.yaml
|   |   +-- spark.yaml
|   +-- prod/
|       +-- job.yaml
|       +-- spark.yaml
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
### Custom Configuration