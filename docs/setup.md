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
  #Amaterasu Setup Guide
  
  ## Download
  
  Amaterasu is available for [download](http://amaterasu.incubator.apache.org/downloads.html) download page.
  You need to download Amaterasu and extract it on to a node in the cluster. Once you do that, you are just a couple of easy steps away from running your first job.
  
  ## Configuration
  
  Configuring amaterasu is simply done buy editing the `amaterasu.properties` file in the top-level amaterasu directory. 
  
  Because Amaterasu supports several cluster environments (currently it supports Apache Mesos and Apache YARN) 
  
  ### Apache Mesos
  
  | property   | Description                    | Value          |
  | ---------- | ------------------------------ | -------------- |
  | Mode       | The cluster manager to be used | mesos          |
  | zk         | The ZooKeeper connection<br> string to be used by<br> amaterasu | The address of a zookeeper node  |
  | master     | The clusters' Mesos master | The address of the Mesos Master  |
  | user       | The user that will be used<br> to run amaterasu | root           |
  
  ### Apache YARN
  
  **Note:**  Different Hadoop distributions need different variations of the YARN configuration. Amaterasu is currently tested regularly with HDP and Amazon EMR. 
  
  
  | property   | Description                    | Value          |
  | ---------- | ------------------------------ | -------------- |
  | Mode       | The cluster manager to be used | mesos          |
  | zk         | The ZooKeeper connection<br> string to be used by<br> amaterasu | The address of a zookeeper node  |
