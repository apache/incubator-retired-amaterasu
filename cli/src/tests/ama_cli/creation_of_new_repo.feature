# Created by nadavh at 05/10/2017
Feature: Support of creating a new Amaterasu job repository
  The feature is support to result in a new git repository that has a very specific structure:
  /root_dir
    |__/src ## This is where the source code resides
    |   |
    |   |__task1.scala
    |   |
    |   |__task2.py
    |   |
    |   |__task3.sql
    |
    |__/env ## This is a configuration directory for each environment the user defines, there should be a "default" env.
    |   |
    |   |__/default
    |   |  |
    |   |  |__job.yml
    |   |  |
    |   |  |__spark.yml
    |   |
    |   |__/test
    |
    |__maki.yml ## The job definition

  Scenario: Invoking the InitRepository handler with a valid path should result in a new git repository
    Given The absolute path "/tmp/amaterasu/test"
    When InitRepository handler is invoked with the given path
    Then A directory with path "/tmp/amaterasu/test" should be created
    And The directory in path "/tmp/amaterasu/test" should be a git repository
    And The "/tmp/amaterasu/test" directory should have a "maki.yml" file
    And The "/tmp/amaterasu/test" directory should have a "src" subdirectory
    And The "/tmp/amaterasu/test" directory should have a "env" subdirectory
    And the "/tmp/amaterasu/test/env" directory should have a "default" subdirectory
    And the "/tmp/amaterasu/test/env/default" directory should have a "job.yml" file
    And the "/tmp/amaterasu/test/env/default" directory should have a "spark.yml" file

  Scenario: Invoking the InitRepository handler with a path that doesn't exist should result in an exception
    Given The invalid absolute path "/aaa/bbb/ccc"
    When InitRepository handler is invoked with the given path
    Then An HandlerError should be raised

  Scenario: Invoking the InitRepository handler with a valid path that is already a empty repository should create all the required Amaterasu file structure
    Given The absolute path "/tmp/amaterasu/test"
    Given The path is a repository
    When InitRepository handler is invoked with the given path
    Then The directory in path "/tmp/amaterasu/test" should be a git repository
    And The "/tmp/amaterasu/test" directory should have a "maki.yml" file
    And The "/tmp/amaterasu/test" directory should have a "src" subdirectory
    And The "/tmp/amaterasu/test" directory should have a "env" subdirectory
    And the "/tmp/amaterasu/test/env" directory should have a "default" subdirectory
    And the "/tmp/amaterasu/test/env/default" directory should have a "job.yml" file
    And the "/tmp/amaterasu/test/env/default" directory should have a "spark.yml" file

  Scenario: Invoking the InitRepository handler with a valid path that is already a repository that is missing a maki file, should only create the maki file
    Given The absolute path "/tmp/amaterasu/test"
    Given The path is a repository
    Given The "/tmp/amaterasu/test" directory has a "src" subdirectory
    Given The "/tmp/amaterasu/test" directory has a "env" subdirectory
    Given The "/tmp/amaterasu/test/env" directory has a "default" subdirectory
    Given The "/tmp/amaterasu/test/env/default" directory has a "job.yml" file
    Given The "/tmp/amaterasu/test/env/default" directory has a "spark.yml" file
    When InitRepository handler is invoked with the given path
    Then The "/tmp/amaterasu/test" directory should have a "maki.yml" file
    And Only "maki.yml" should have changed

  Scenario: Invoking the InitRepository handler with a valid path that is already a repository that is missing the env directory, should only create the env directory
    Given The absolute path "/tmp/amaterasu/test"
    Given The path is a repository
    Given The "/tmp/amaterasu/test" directory has a "src" subdirectory
    Given The "/tmp/amaterasu/test" directory has a "maki.yml" file
    When InitRepository handler is invoked with the given path
    Then The "/tmp/amaterasu/test" directory should have a "env" subdirectory
    And the "/tmp/amaterasu/test/env" directory should have a "default" subdirectory
    And the "/tmp/amaterasu/test/env/default" directory should have a "job.yml" file
    And the "/tmp/amaterasu/test/env/default" directory should have a "spark.yml" file
    And Only "env,env/default,env/default/job.yml,env/default/spark.yml" should have changed

