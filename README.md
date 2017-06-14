#Amaterasu

                                               /\
                                              /	 \ /\
                                             / /\ /  \
        _                 _                 / /  / /\ \   
       /_\   _ __   __ _ | |_  ___  _ _  __(_( _(_(_ )_) 
      / _ \ | '  \ / _` ||  _|/ -_)| '_|/ _` |(_-<| || |
     /_/ \_\|_|_|_|\__,_| \__|\___||_|  \__,_|/__/ \_,_|
                                                        

Amaterasu is an open-source, deployment tool for data pipelines. Amaterasu allows developers to write and easily deploy data pipelines, and clusters manage their configuration and dependencies.

## Download

For this preview version, we have packaged amaterasu nicely for you to just [download](https://s3-ap-southeast-2.amazonaws.com/amaterasu/amaterasu.tgz) and extract.
Once you do that, you are just a couple of easy steps away from running your first job.

## Creating a dev/test Mesos cluster

We have also created a Mesos cluster you can use to test Amaterasu or use for development purposes.
For more details, visit the [amaterasu-vagrant](https://github.com/shintoio/amaterasu-vagrant) repo

## Configuration

Configuring amaterasu is very simple. Before running amaterasu, open the `amaterasu.properties` file in the top-level amaterasu directory, and verify the following properties:

| property   | Description                | Default value  |
| ---------- | -------------------------- | -------------- |
| zk         | The ZooKeeper connection<br> string to be used by<br> amaterasu | 192.168.33.11  |
| master     | The clusters' Mesos master | 192.168.33.11  |
| user       | The user that will be used<br> to run amaterasu | root           |

## Running a Job

To run an amaterasu job, run the following command in the top-level amaterasu directory:

```
ama-start.sh --repo="https://github.com/shintoio/amaterasu-job-sample.git" --branch="master" --env="test" --report="code" 
```

We recommend you either fork or clone the job sample repo and use that as a starting point for creating your first job.

# Amaterasu Developers Information 

## Building Amaterasu

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

                      

