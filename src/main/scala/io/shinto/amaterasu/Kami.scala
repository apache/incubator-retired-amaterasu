package io.shinto.amaterasu

import java.util.concurrent.{ConcurrentHashMap, BlockingQueue, LinkedBlockingQueue}
import com.github.nscala_time.time.Imports._
import io.shinto.amaterasu.dataObjects.Job

import scala.collection.JavaConversions

/**
 * This class is the cluster manager for Amaterasu (a goddess object), managing the state of the cluster:
 * - The jobsQueue containing all waiting jobs
 * - completedJobs
 * - failedJobs
 */
class Kami {

  private[amaterasu] var frameworkId: String = null
  private var jobsQueue: BlockingQueue[Job] = null
  private var completedJobs: ConcurrentHashMap[String, Job] = new ConcurrentHashMap[String, Job]()
  private var failedJobs: ConcurrentHashMap[String, Job] = new ConcurrentHashMap[String, Job]()

  def getQueuedJobs(): Array[Job] = {

    jobsQueue.toArray(new Array[Job](0))

  }

  /**
   * Queues a job for execution
   * @param jobUrl the url of the git repo containing the job definition
   * @return the id of the newly created job
   */
  def addJob(jobUrl: String): String = {

    val id = s"job_${System.currentTimeMillis()}"
    jobsQueue.put(Job(
      src = jobUrl,
      id = id,
      timeCreated = DateTime.now,
      startTime = null,
      endTime = null
    ))

    id

  }

  def addJob(job: Job): Unit = {

    jobsQueue.put(job)

  }

  def getNextJob(): Job = {

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
    goddess.jobsQueue = new LinkedBlockingQueue[Job]()

    if (jobs != null) {

      goddess.jobsQueue.addAll(JavaConversions.asJavaCollection(jobs.map(j => Job(
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