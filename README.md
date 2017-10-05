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
# Apache Amaterasu [![Build Status](https://travis-ci.org/apache/incubator-amaterasu.svg?branch=master)](https://travis-ci.org/apache/incubator-amaterasu)

                                               /\
                                              /  \ /\
                                             / /\ /  \
        _                 _                 / /  / /\ \   
       /_\   _ __   __ _ | |_  ___  _ _  __(_( _(_(_ )_) 
      / _ \ | '  \ / _` ||  _|/ -_)| '_|/ _` |(_-<| || |
     /_/ \_\|_|_|_|\__,_| \__|\___||_|  \__,_|/__/ \_,_|
                                                        

Apache Amaterasu is an open-source, deployment tool for data pipelines. Amaterasu allows developers to write and easily deploy data pipelines, and clusters manage their configuration and dependencies.
 

### Supported cluster managers

Currently Apache Amaterasu supports the following cluster managers:
1. Apache Mesos
    > Important! Does not support DC/OS at the moment! only standalone deployment of mesos.
2. Apache Hadoop YARN
    > Due to a bug, we do not support Micorosft Azure HDInsight at the moment.


### Requirements

Here is the list of requirements you will need to have installed on your cluster's master node.

1. Java 1.8 (Oracle or OpenJDK, either is OK)
1. Python 3.3+
2. [pip](https://pip.pypa.io/en/stable/installing/)

### Installation

First, on your cluster's master node, download the latest Apache Amaterasu [distributable](https://github.com/apache/incubator-amaterasu/releases/latest).

Next, unpack the tarball:

```bash
tar -xzf apache-amaterasu-<version>.tar amaterasu
```

Next install the CLI:

```
cd amaterasu
sudo pip install ./cli
```

After installation you will need to setup Amaterasu. You can do it using the `ama setup` command.

The setup phase is dictated by the cluster manager you use.
So for example, to setup Amaterasu for a mesos cluster, you would do:
```bash
ama setup mesos
```

During step you will be asked to configure a few parameters, the setup will result in the following happening:
1. The Amaterasu distributable will be downloaded and deployed. 
2. `amaterasu.properties` file will be created in the current user's home directory. This is a configuration file that feeds the distributable.
3. Dependencies will be downloaded (miniconda and spark for a Mesos cluster and only miniconda for a Hadoop cluster)

## Creating a dev/test Mesos cluster

We have also created a Mesos cluster you can use to test Amaterasu or use for development purposes.
For more details, visit the [amaterasu-vagrant](https://github.com/shintoio/amaterasu-vagrant) repo

## Creating a job repository

Amaterasu has a very specific definition for how an Amaterasu compliant job repository should look like. For this, we supply the `ama init` command.

Here are the steps to properly create an Apache Amaterasu job repository:
1. Start by creating a new directory, for example "ExampleJobRepo"
2. `cd ExampleJobRepo`
3. Run `ama init`

This will create a new repository inside the ExampleJobRepo directory. The new repository will include the following structure:

1. **maki.yml** - This is the job instructions file, more on this below.
2. **src** - A directory for source files that are used as part of the job. Currently we support Spark with Scala, Python, R and SQL bindings.
3. **env** - A directory for environment specific configuration files. You may want to have different configurations for development and production environments
4. **deps** - A directory for files that list software dependencies that the job requires. E.g. - numpy, pandas, etc.

After you've created the ExampleJobRepo, fill it in with your source code, environment configurations,  you will need to push it to a remote git host that your cluster has access to. 
That's it, you are set to run your first job.

## Running a Job

To run a job using Amaterasu, we prepared a nifty little CLI command.

```bash
ama run
```

The ama run commands receives a mandatory job repository URL.

```bash
ama run <repository_url>

# e.g.

ama run https://github.com/shintoio/amaterasu-job-sample.git
```

Unless specified otherwise, we use the master branch. If you want to change this, you can add the ```-b``` flag.

```bash
ama run <repository_url> -b <your_branch>

# e.g.

ama run https://github.com/shintoio/amaterasu-job-sample.git -b python-support
```


#### Execution Environments

So you are running Apache Amaterasu, that's great! But maybe, you'd want to specify some job or environment related arguments for Apache Amaterasu to pass on to the underlying cluster manager.
For example, maybe for production you want to set the spark executor memory on HDP to 1g, you can do it by adding it to a ```env/hdp-prod/spark.yml``` file.
Maybe during the testing phase, you'd like Spark driver to only use 2 cores, this is why you'd want to have different environment for testing.

So let's assume that you ended up creating 2 environments - ```hdp-prod``` and ```hdp-test```.

To use the ```hdp-prod``` environment, simply run with the ```-e``` flag like this:

```
ama run <repository_url> -e <environment>

# e.g. 
ama run https://github.com/shintoio/amaterasu-job-sample.git -e hdp-prod
```

> Note - If you don't specify any environment, Amaterasu will use the "default" environment.

For more CLI options, use the builtin help (```ama -h```)

It is highly recommended that you take a peek at our [sample job repository](https://github.com/shintoio/amaterasu-job-sample.git) before using Amaterasu.


# Apache Amaterasu Developers Information 

## Building Apache Amaterasu

### Instructions to download gradle wrapper when building from source distribution

As part of the ASFs policy, when building Apache Amaterasu from a source distribution and not from git, the gradle-wrapper.jar needs to be downloaded.
The following command, will download the gradle wrapper from Amaterasu git repository and puts under incubator-amaterasu/gradle/wrapper

wget --no-check-certificate -P incubator-amaterasu/gradle/wrapper https://github.com/apache/incubator-amaterasu/raw/version-0.2.0-incubating-rc4/gradle/wrapper/gradle-wrapper.jar
(or)
curl --insecure -L https://github.com/apache/incubator-amaterasu/raw/version-0.2.0-incubating-rc4/gradle/wrapper/gradle-wrapper.jar > incubator-amaterasu/gradle/wrapper/gradle-wrapper.jar

### Building Amaterasu with Gradle

to build the amaterasu home dir (for dev purposes) run:
```
./gradlew buildHomeDir test
```

to create a distributable jar (clean creates the home dir first) run:
```
./gradlew buildDistribution test
```

## Architecture

Amaterasu is an Apache Mesos framework with two levels of schedulers:

* The ClusterScheduler manages the execution of all the jobs
* The JobScheduler manages the flow of a job

The main clases in Amateraso are listed bellow:

    +-------------------------+   +------------------------+
    | ClusterScheduler        |   | Kami                   |
    |                         |-->|                        |
    | Manage jobs:            |   | Manages the jobs queue |
    | Queue new jobs          |   | and Amaterasu cluster  |
    | Reload interrupted jobs |   +------------------------+
    | Monitor cluster state   |
    +-------------------------+
                |
                |     +------------------------+
                |     | JobExecutor            |
                |     |                        |
                +---->| Runs the Job Scheduler |
                      | Communicates with the  |
                      | ClusterScheduler       |
                      +------------------------+
                                 |
                                 |
                      +------------------------+      +---------------------------+                      
                      | JobScheduler           |      | JobParser                 |
                      |                        |      |                           |
                      | Manages the execution  |----->| Parses the kami.yaml file |
                      | of the job, by getting |      | and create a JobManager   |
                      | the  execution flow    |      +---------------------------+
                      | fron the JobManager    |                    |
                      | and comunicating with  |      +---------------------------+
                      | Mesos                  |      | JobManager                |                      
                      +------------------------+      |                           |
                                 |                    | Manages the jobs workflow |
                                 |                    | independently of mesos    |
                      +------------------------+      +---------------------------+
                      | ActionExecutor         |
                      |                        |
                      | Executes ActionRunners |
                      | and manages state for  |
                      | the executor           |
                      +------------------------+

                      

