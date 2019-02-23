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

Amaterasu Deployments are a combination of yaml deployments definition (usually defined in the `maki.yml` or `maki.yaml` file), the environment configuration (described in the configuration section) and artifacts to be deployed.

### The Deployments DSL
The deployment DSL, allows developers to define Actions to be deployed, their order of deployment and execution using a simple YAML definition. The following example:

```yaml
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
    - name: step2
      runner:
          group: spark
          type: pyspark
      file: file.py
      
```

The above deployment, defines two actions which will run sequantially. Each action, defines a [framework](../frameworks/) runner to be used and an executable to be run.

### Executables
Cureently, Amaterasu actions can define two types of executables:

 - **Files** 
   
File executables can be located inside the Amaterasu repo, under the `src` folder, for example, in the following action definition:
 
 
```yaml
job-name:    amaterasu-test
seq:     
    - name: step1
      runner:
         group: spark
         type: pyspark
      file: file.py
```
     
the executable `file.py` would be located under the src folder as follows:

```yaml
repo/
+-- src/
|   +-- file.py
+-- maki.yaml

```          

Files can also be specified as URLs, where currently the `http`, `https` and `s3a` schemas are supported at this time for example:

```yaml
job-name:    amaterasu-test
seq:     
    - name: step1
      runner:
         group: spark
         type: pyspark
      file: s3a://my-source-bucket/file.py
```

 - **Artifacts** 
 
Currently, the artifact directive supports only artifacts stored in Maven repositories. In addition to the artifact details, you will need to specify the details of the repository where the artifact is available. The following example, fetches an artifact to be submitted as a spark job:

```yaml
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
```
#### Error Handling Actions

When an action fails, Amaterasu will re-queue that action for execution a configurable number of times. If the action continues to fail, Amaterasu allows for the definition of error handling actions that will execute when an action fails repeatedly. The following deployment defines an error handling action. 

```yaml
job-name:    amaterasu-sample
flow:
    - name: start
      runner:
         group: spark
         type: pyspark
      file: file.py
      error:        
         name: error
         runner:
            group: spark
            type: pyspark
         file: error.py        

```
 
## Dependencies

 In addition to defining executables, Amaterasu jobs and actions can define dependencies to be deployed in the containers and used in runtime. Dependencies can be defined either globally for a job, under the `deps` folder, or per action in the action folder:
 
```
repo
+-- deps/
|   +-- jars.yaml          #contains golbal depenedencies which are deployed in all action containers
+-- src/
|   +-- start/
|   |   +-- jars.yaml      #contains depenedencies which are deployed only in the start action container
|   +-- file.py
+-- maki.yaml 

```

 - Java/JVM Dependencies
 
JVM dependencies are defined in the `jars.yaml` file, the file defines a set of dependencies and repositories where those dependencies are available. THe following example shows a simple `jars.yaml` file:

```yaml
repos:
  - id: maven-central
    type: default
    url: http://central.maven.org/maven2/
artifacts:  
  - groupId: com.flyberrycapital
    artifactId: scala-slack_2.10
    version: 0.3.0

```
 
 - Python Dependencies