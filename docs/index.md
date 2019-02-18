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

# Apache Amaterasu (incubating) [![Build Status](https://travis-ci.org/apache/incubator-amaterasu.svg?branch=master)](https://travis-ci.org/apache/incubator-amaterasu)

![Apache Amaterasu](images/amaterasu-logo-web.png)                                                        

Apache Amaterasu is an open-source framework providing configuration management and deployment for Data pipelines. Amaterasu allows developers and data scientists to write, collaborate and easily deploy data pipelines to different cluster environments. Amaterasu allows them manage configuration and dependencies for different environments.

## Running a Job

To run an amaterasu job, run the following command in the top-level amaterasu directory:

```
ama-start.sh --repo="https://github.com/shintoio/amaterasu-job-sample.git" --branch="master" --env="test" --report="code" 
```

We recommend you either fork or clone the job sample repo and use that as a starting point for creating your first job.

# Apache Amaterasu Developers Information 

## Building Apache Amaterasu

to build the amaterasu home dir (for dev purposes) run:
```
./gradlew buildHomeDir test
```

to create a distributable jar (clean creates the home dir first) run:
```
./gradlew buildDistribution test
```
