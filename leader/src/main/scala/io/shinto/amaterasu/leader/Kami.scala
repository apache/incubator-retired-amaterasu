package io.shinto.amaterasu

import java.util.concurrent.{ ConcurrentHashMap, BlockingQueue, LinkedBlockingQueue }
import com.github.nscala_time.time.Imports._
import io.shinto.amaterasu.common.dataobjects.JobData

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