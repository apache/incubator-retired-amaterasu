package org.apache.amaterasu.leader.utilities


case class Args(
                 repo: String = "",
                 branch: String = "master",
                 env: String = "default",
                 name: String = "amaterasu-job",
                 jobId: String = null,
                 report: String = "code",
                 home: String = "",
                 newJobId: String = ""
               ) {
  def toCmdString: String = {
    var cmd = s""" --repo $repo --branch $branch --env $env --name $name --report $report --home $home"""
    if(jobId != null && !jobId.isEmpty) {
      cmd += s" --job-id $jobId"
    }
    cmd
  }

  override def toString: String = {
    toCmdString
  }
}

object Args {
  def getParser: scopt.OptionParser[Args] = {
    val pack = this.getClass.getPackage
    new scopt.OptionParser[Args]("amaterasu job") {

      head("amaterasu job", if(pack == null) "DEVELOPMENT" else pack.getImplementationVersion)

      opt[String]('r', "repo") action { (x, c) =>
        c.copy(repo = x)
      } text "The git repo containing the job"

      opt[String]('b', "branch") action { (x, c) =>
        c.copy(branch = x)
      } text "The branch to be executed (default is master)"

      opt[String]('e', "env") action { (x, c) =>
        c.copy(env = x)
      } text "The environment to be executed (test, prod, etc. values from the default env are taken if np env specified)"

      opt[String]('n', "name") action { (x, c) =>
        c.copy(name = x)
      } text "The name of the job"

       opt[String]('i', "job-id") action { (x, c) =>
        c.copy(jobId = x)
      } text "The jobId - should be passed only when resuming a job"

      opt[String]('j', "new-job-id") action { (x, c) =>
        c.copy(newJobId = x)
      } text "A new jobId - should never be passed by a user"

      opt[String]('r', "report") action { (x, c) =>
        c.copy(report = x)
      }  text "The level of reporting"

      opt[String]('h', "home") action { (x, c) =>
        c.copy(home = x)
      }
    }
  }
}
