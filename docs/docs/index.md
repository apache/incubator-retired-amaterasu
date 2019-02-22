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

## Main concepts

### Repo

Amaterasu jobs are defined within and Amaterasu repository. A repository is a filesystem structure stored in a git repository that contains definitions for the following components: 
 
#### Actions

Put simply, an action is a process that is being managed by Amaterasu. In order to deploy and manage an actions Amaterasu is creating a container with the action, its dependencies and configuration, and deploys it on a cluster (currently either only Apache Mesos and YARN clusters are supported).

#### Frameworks

Apache Amaterasu is able to configure and interact with different data processing frameworks. Supported frameworks can be easily configured for deployment, and also integrate seamlessly with custom APIs. 
For more information about supported frameworks and how to support additional frameworks seeour  [Frameworks](frameworks/) section.

#### Configuration and Environments  

One of the main objectives of Amaterasu is to manage configuration [configuration](config/) for data pipelines. Amaterasu configurations are stored per environment allowing the same pipeline to be deployed with a configuration that fits it's environment.

#### Deployments

Amaterasu [deployments](deployments/) are stored in a `maki.yml` or `maki.yaml` file in the root of the amaterasu repository. The deployment definition contains the different actions, and their order of deployment and execution.

## Setting up Amaterasu  

### Download

Amaterasu is available for [download](http://amaterasu.incubator.apache.org/downloads.html) download page.
You need to download Amaterasu and extract it on to a node in the cluster. Once you do that, you are just a couple of easy steps away from running your first job.

### Configuration

Configuring amaterasu is simply done buy editing the `amaterasu.properties` file in the top-level amaterasu directory. 

Because Amaterasu supports several cluster environments (currently it supports Apache Mesos and Apache YARN) 

#### Apache Mesos

| property   | Description                    | Value          |
| ---------- | ------------------------------ | -------------- |
| Mode       | The cluster manager to be used | mesos          |
| zk         | The ZooKeeper connection<br> string to be used by<br> amaterasu | The address of a zookeeper node  |
| master     | The clusters' Mesos master | The address of the Mesos Master  |
| user       | The user that will be used<br> to run amaterasu | root           |

#### Apache YARN

**Note:**  Different Hadoop distributions need different variations of the YARN configuration. Amaterasu is currently tested regularly with HDP and Amazon EMR. 


| property   | Description                    | Value          |
| ---------- | ------------------------------ | -------------- |
| Mode       | The cluster manager to be used | mesos          |
  | zk         | The ZooKeeper connection<br> string to be used by<br> amaterasu | The address of a zookeeper node  |


## Running a Job

To run an amaterasu job, run the following command in the top-level amaterasu directory:

```
ama-start.sh --repo="https://github.com/shintoio/amaterasu-job-sample.git" --branch="master" --env="test" --report="code" 
```

We recommend you either fork or clone the job sample repo and use that as a starting point for creating your first job.