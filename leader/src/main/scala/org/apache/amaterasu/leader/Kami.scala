/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.amaterasu.leader

import java.util.concurrent.{BlockingQueue, ConcurrentHashMap, LinkedBlockingQueue}

import com.github.nscala_time.time.Imports._
import org.apache.amaterasu.common.dataobjects.JobData

import scala.collection.JavaConversions

/**
  * This class is the cluster manager for Amaterasu (a goddess object), managing the state of the cluster:
  * - The jobsQueue containing all waiting jobs
  * - completedJobs
  * - failedJobs
  */
class Kami {

  private[amaterasu] var frameworkId: String = null
  private var jobsQueue: BlockingQueue[JobData] = null
  private var completedJobs: ConcurrentHashMap[String, JobData] = new ConcurrentHashMap[String, JobData]()
  private var failedJobs: ConcurrentHashMap[String, JobData] = new ConcurrentHashMap[String, JobData]()

  def getQueuedJobs(): Array[JobData] = {

    jobsQueue.toArray(new Array[JobData](0))

  }

  /**
    * Queues a job for execution
    * @param jobUrl the url of the git repo containing the job definition
    * @return the id of the newly created job
    */
  def addJob(jobUrl: String): String = {

    val id = s"job_${System.currentTimeMillis()}"
    jobsQueue.put(JobData(
      src = jobUrl,
      id = id,
      timeCreated = DateTime.now,
      startTime = null,
      endTime = null
    ))

    id

  }

  def addJob(job: JobData): Unit = {

    jobsQueue.put(job)

  }

  def getNextJob(): JobData = {

    jobsQueue.take()

  }

}

object Kami {

  /**
    *
    * @param jobs
    * @return
    */
  def apply(jobs: Seq[String]): Kami = {

    val goddess = new Kami()
    goddess.jobsQueue = new LinkedBlockingQueue[JobData]()

    if (jobs != null) {

      goddess.jobsQueue.addAll(JavaConversions.asJavaCollection(jobs.map(j => JobData(
        src = j,
        id = s"job_${System.currentTimeMillis()}",
        timeCreated = DateTime.now,
        startTime = null,
        endTime = null
      ))))

    }

    goddess
  }

  def apply(): Kami = {

    apply(null)

  }
}