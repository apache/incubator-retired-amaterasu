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
      
    The env object contains the configuration for the current environment.
    
    
    

   - **Datasets and Dataset configuration**
    
    While [datasets](config/#datasets/) are configured under an environment, Amaterasu datasets are treated differently from other configurations, as they provide the integration point between different actions. Datasets can be either consumed as a configuration or to be loaded directly into an appropriate data structure for the specific framework and runtime. 

# Amaterasu Frameworks

## Python 
Amaterasu supports a variety of Python frameworks:

1. PySpark workload ([See below](#pyspark))

2. Pandas workload 

3. Generic Python workload

Each workload type has a dedicated Apache Amaterasu SDK. 
The Apache Amaterasu SDK is available in PyPI and can be installed as follows:
```bash
pip install apache-amaterasu
```

Alternatively, it is possible to download the SDK source and manually install it via ```easy_install``` or executing the setup script.

```bash
wget <link to source distribution>
tar -xzf apache-amaterasu-0.2.1-incubating.tar.gz
cd apache-amaterasu-0.2.1-incubating
python setup.py install
```

### Action dependencies
Apache Amaterasu has the capability of ensuring Python dependencies are present on all execution nodes when executing action sources.

In order to define the required dependencies, a ```requirements.txt``` file has to be added to the job repository.
Currently, only a global ```requirements.txt``` is supported.

Below you can see where the requirements file has to be added:
```
repo
+-- deps/
|   +-- requirements.txt <-- This is the place where you define python dependendencies
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

When a ```requirements.txt``` file exists, Apache Amaterasu distributes it to the execution containers and locally installs the dependencies in each container.

> **Important** - Your execution nodes need to have egress connection available in order to use pip

### Pandas
### Generic Python


## Java and JVM programs

## Apache Spark

### Spark Configuration
Apache Amaterasu has the capability of deploying Spark applications and provide configuration and integration 
utilities that work in the realm of dataframes and RDDs.

### Scala
### PySpark
Assuming that Apache Amaterasu has been [configured](./config.md) correctly for your cluster and all required 
spark [configurations](#spark-configuration) are in place, all you need to do is integrate with the Apache Amaterasu 
PySpark SDK from within your source scripts.

#### Integration with supported data sources

The Spark SDK provides a mechanism to seamlessly consume and persist datasets. To do so, you must first define a [dataset configuration](./config.md#datasets). 
Let's assume that you have a 2 datasets defined. The first, ```mydataset``` is the input dataset and ```resultdataset```  is the output dataset.
Let's also assume that you have 2 different environments, production and development.
Let's take a sneak peek at an example of how ```mydataset``` is defined in each environment:

__production__
```yaml
file:
    url: s3a://myprodbucket/path/myinputdata
    format: parquet
```
    
    
___development__
```yaml
file:
    url: s3a://mydevbucket/path/myinputdata
    format: parquet
```
     
The snippet below shows how to use the Amaterasu SDK to integrate with the data sources easily.

```python
from amaterasu.pyspark.runtime import AmaContext

ama_context = AmaContext.builder().build()
some_dataset: Dataframe = ama_context.load_dataset("mydataset")
... # Do some black magic here
ama_context.persist_dataset("resultdataset", some_dataset_after_transformation)
```

The code above will work regardless of whether you run in development environment or production.
> Important: You will need to have the datasets above defined in both environments!

#### Integration with unsupported data sources

Apache Amaterasu is still an evolving project, we add more and more builtin integrations as we go (e.g. - Pandas dataframes, new datasource types and so on). 
As we are aware of the need to manage datasets across environments, regardless of whether Apache Amaterasu supports them by default or not, we designed Apache Amaterasu to provide a way to define [generic datasets](./config.md#generic-datasets). 
In addition to defining generic datasets, Apache Amaterasu also provides an API to reference any dataset configuration defined in the environment.

The snippet below shows an example of referencing a dataset configuration in the code.

```python
from amaterasu.pyspark.runtime import AmaContext

ama_context = AmaContext.builder().build()
dataset_conf = ama_context.dataset_manager.get_dataset_configuration("mygenericdataset")  # This is a dataset without builtin support

some_prop = dataset_conf['mydataset_prop1']
some_prop2 = dataset_conf['mydataset_prop2']

my_df = my_custom_loading_logic(some_prop, some_prop2)
my_new_df = black_magic(my_df)
ama_context.persist('magic_df', my_new_df)  # This is a dataset with builtin support

```


      

