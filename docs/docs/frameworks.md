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
# Overview

Amaterasu supports different processing frameworks to be executed. Amaterasu frameworks provides two main components for integrating with such frameworks:

 - **Dispatcher** 
 
   The dispatcher is in charge of creating and configuring a containers for actions of a specific framework. It makes sure that the executable and any dependencies are available in the container, as well as the environment configuration files, and sets the command to be executed.  
   
 - **Runtime Library**
   
   The runtime library provide an easy way to consume environment configuration and share data between actions. The main entry point for doing so is using the Amaterasu Context object. Amaterasu Context exposes the following functionality:
   
   **Note:** Each runtime (Java, Python, etc.) and framework have slightly different implementation of the Amaterasu context. To develop using a specific Framework, please consult the frameworks documentation bellow.
    - **Env**
      
      The `env` object contains the configuration for the current environment.
      
    - **Datasets and Dataset configuration**
    
      While [datasets](config/#datasets/) are configured under an environment, Amaterasu datasets are treated differently from other configurations, as they provide the integration point between different actions. Datasets can be either consumed as a configuration or to be loaded directly into an appropriate data structure for the specific framework and runtime. 

# Amaterasu Frameworks

## Apache Spark

### Spark Configuration

### Scala
### PySpark

## Python 

## Java and JVM programs