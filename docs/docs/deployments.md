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
#Defining Deployments

##Overview

Amaterasu Deployments are a combination of yaml deployments definition (usually defined in the `maki.yml` or `maki.yaml` file), the environment configuration (described in the configuration section) and artifacts to be deployed.

###The Deployments DSL
The deployment DSL, allows developers to define Actions to be deployed, their order of deployment and execution using a simple YANL definition. The following example:

```yaml
---
job-name:    amaterasu-test
seq:
    - name: start
      runner:
          group: spark
          type: jar        
      artifact: 
          groupId: io.shinto
          artifactId: amaterasu-simple-spark
          version: 0.3
      repo:
          id: packagecloud
          type: default
          url: https://packagecloud.io/yanivr/amaterasu-demo/maven2
      class: DataGenerator
      exports:
          odd: parquet
    - name: step2
      runner:
          group: spark
          type: pyspark
      file: file.py
...
```

The above deployment, defines two actions which will run sequantially. Each action, is ran