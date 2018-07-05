Feature: Run an amaterasu pipeline


  Scenario: Run a pipeline on Mesos for a valid repository, should not raise an error and produce a valid command

    Given A valid repository
    When Running a pipeline on Mesos with the given repository
    Then An HandlerError should not be raised
    And The resulting command looks like this
      """
      java -cp /tmp/amaterasu/assets/bin/leader-0.2.0-incubating-rc3-all.jar -Djava.library.path=/usr/lib org.apache.amaterasu.leader.mesos.MesosJobLauncher --home /tmp/amaterasu/assets --repo http://git.sunagakure.com/ama-job-valid.git --env default --report code --branch master --config-home /tmp/amaterasu
      """

  Scenario: Run a pipeline on YARN for a valid repository, should not raise an error and produce a valid command
    Given A valid repository
    When Running a pipeline on YARN with the given repository
    Then An HandlerError should not be raised
    And The resulting command looks like this
      """
      yarn jar /tmp/amaterasu/assets/bin/leader-0.2.0-incubating-rc3-all.jar org.apache.amaterasu.leader.yarn.Client --home /tmp/amaterasu/assets --repo http://git.sunagakure.com/ama-job-valid.git --env default --report code --branch master --config-home /Users/nadavh/.amaterasu
      """

  Scenario: Run a pipeline for a repository that doesn't exist, should raise an error
    Given A repository that doesn't exist
    When Running a pipeline on Mesos with the given repository
    Then An HandlerError should be raised

  Scenario: Run a pipeline for a repository that is not amaterasu compliant, should raise an error
    Given A repository that is not Amaterasu compliant
    When Running a pipeline on Mesos with the given repository
    Then An HandlerError should be raised

  Scenario: Run a pipeline for a valid repository by file URI should raise an error
    Given A valid file URI repository
    When Running a pipeline on Mesos with the given repository
    Then Amaterasu should run