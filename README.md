#Amaterasu

       (                      )
       )\        )      )   ( /(   (   (       )        (
      ((_)(     (     ( /(  )\()  ))\  )(   ( /(  (    ))\
     )\ _ )\    )\  ' )(_))(_))/ /((_)(()\  )(_)) )\  /((_)
     (_)_\(_) _((_)) ((_) _ | |_ (_))   ((_)((_)_ ((_)(_))(
      / _ \  | '   \()/ _` ||  _|/ -_) | '_|/ _` |(_-<| || |
     /_/ \_\ |_|_|_|  \__,_| \__|\___| |_|  \__,_|/__/ \_,_|

Amateraso is an open-source, distributed dataflow framework, that allows developers to build long running data-processing pipelines using a variety of standard data processing frameworks, including Apache Spark, Apache Flink and more.

##Architecture

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
                      
## Running a Job

To run an amaterasu job, run the following command:

```
java -cp /ama/amaterasu-assembly-0.1.0.jar -Djava.library.path=/usr/lib io.shinto.amaterasu.mesos.JobLauncher --repo "https://github.com/roadan/amaterasu-job-sample.git" --branch master
```