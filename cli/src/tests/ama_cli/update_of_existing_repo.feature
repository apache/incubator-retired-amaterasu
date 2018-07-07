"""
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
Feature: Updating an existing repository from a maki file.

  Scenario: Updating an non existing repository, should throw an error
    Given The relative path "tmp/amaterasu"
    Given The "tmp/amaterasu" directory has a valid maki file
    When Updating the repository using the maki file
    Then An HandlerError should be raised
    And The "tmp/amaterasu/src" shouldn't have a "example.py" file
    And The "tmp/amaterasu/src" shouldn't have a "example.scala" file

  Scenario: Updating an existing repository from an empty maki, should throw an error
    Given The relative path "tmp/amaterasu"
    Given The path is a repository
    Given The "tmp/amaterasu" directory has a "src" subdirectory
    Given The "tmp/amaterasu" directory has a "env" subdirectory
    Given The "tmp/amaterasu/env" directory has a "default" subdirectory
    Given The "tmp/amaterasu" directory has an empty maki file
    When Updating the repository using the maki file
    Then An HandlerError should be raised
    And The "tmp/amaterasu/src" shouldn't have a "example.py" file
    And The "tmp/amaterasu/src" shouldn't have a "example.scala" file

  Scenario: Updating an existing repository from an invalid maki, should throw an error
    Given The relative path "tmp/amaterasu"
    Given The path is a repository
    Given The "tmp/amaterasu" directory has an invalid maki file
    Given The "tmp/amaterasu" directory has a "src" subdirectory
    Given The "tmp/amaterasu" directory has a "env" subdirectory
    Given The "tmp/amaterasu/env" directory has a "default" subdirectory
    When Updating the repository using the maki file
    Then An HandlerError should be raised
    And The "tmp/amaterasu/src" shouldn't have a "example.py" file
    And The "tmp/amaterasu/src" shouldn't have a "example.scala" file

  Scenario: Updating an existing repository from another invalid maki, should throw an error
    Given The relative path "tmp/amaterasu"
    Given The path is a repository
    Given The "tmp/amaterasu" directory has another invalid maki file
    Given The "tmp/amaterasu" directory has a "src" subdirectory
    Given The "tmp/amaterasu" directory has a "env" subdirectory
    Given The "tmp/amaterasu/env" directory has a "default" subdirectory
    When Updating the repository using the maki file
    Then An HandlerError should be raised
    And The "tmp/amaterasu/src" shouldn't have a "example.py" file
    And The "tmp/amaterasu/src" shouldn't have a "example.scala" file


  Scenario: Updating an existing repository with no sources from a valid maki, should created new sources
    Given The relative path "tmp/amaterasu"
    Given The path is a repository
    Given The "tmp/amaterasu" directory has a "src" subdirectory
    Given The "tmp/amaterasu" directory has a "env" subdirectory
    Given The "tmp/amaterasu/env" directory has a "default" subdirectory
    Given The "tmp/amaterasu" directory has a valid maki file
    When Updating the repository using the maki file
    Then An HandlerError should not be raised
    And The "tmp/amaterasu/src" directory should have a "example.py" file
    And The "tmp/amaterasu/src" directory should have a "example.scala" file

  Scenario: Updating an existing repository with sources from a valid maki, and the sources in the repo are a subset of the ones in the maki, then new sources should be created
    Given The relative path "tmp/amaterasu"
    Given The path is a repository
    Given The "tmp/amaterasu" directory has a "src" subdirectory
    Given The "tmp/amaterasu" directory has a "env" subdirectory
    Given The "tmp/amaterasu/env" directory has a "default" subdirectory
    Given The "tmp/amaterasu" directory has a valid maki file
    Given The "tmp/amaterasu/src" directory has a "example.scala" file
    When Updating the repository using the maki file
    Then An HandlerError should not be raised
    And The "tmp/amaterasu/src" directory should have a "example.py" file

  Scenario: Updating an existing repository with sources from a valid maki, and the sources in the repo are the same as the ones in the maki, then nothing should happen
    Given The relative path "tmp/amaterasu"
    Given The path is a repository
    Given The "tmp/amaterasu" directory has a "src" subdirectory
    Given The "tmp/amaterasu" directory has a "env" subdirectory
    Given The "tmp/amaterasu/env" directory has a "default" subdirectory
    Given The "tmp/amaterasu" directory has a valid maki file
    Given The "tmp/amaterasu/src" directory has a "example.scala" file
    Given The "tmp/amaterasu/src" directory has a "example.py" file
    When Updating the repository using the maki file
    Then An HandlerError should not be raised

  Scenario: Updating an existing repository with sources from a valid maki, and the sources in the repo are a superset of the ones in the maki, the user is prompted to take action and chooses to keep the file and not update the maki, then nothing should happen
    Given The relative path "tmp/amaterasu"
    Given The path is a repository
    Given The "tmp/amaterasu" directory has a "src" subdirectory
    Given The "tmp/amaterasu" directory has a "env" subdirectory
    Given The "tmp/amaterasu/env" directory has a "default" subdirectory
    Given The "tmp/amaterasu" directory has a valid maki file
    Given The "tmp/amaterasu/src" directory has a "example.scala" file
    Given The "tmp/amaterasu/src" directory has a "example.py" file
    Given The "tmp/amaterasu/src" directory has a "example.sql" file
    When Updating the repository using the maki file, with user keeping source files that are not in the maki
    Then An HandlerError should not be raised


  Scenario: Updating an existing repository with sources from a valid maki, and the sources in the repo are a superset of the ones in the maki, the user chooses to not keep the files, then the extra files should be deleted
    Given The relative path "tmp/amaterasu"
    Given The path is a repository
    Given The "tmp/amaterasu" directory has a "src" subdirectory
    Given The "tmp/amaterasu" directory has a "env" subdirectory
    Given The "tmp/amaterasu/env" directory has a "default" subdirectory
    Given The "tmp/amaterasu" directory has a valid maki file
    Given The "tmp/amaterasu/src" directory has a "example.scala" file
    Given The "tmp/amaterasu/src" directory has a "example.py" file
    Given The "tmp/amaterasu/src" directory has a "example.sql" file
    Given The "tmp/amaterasu/src" directory has a "example.R" file
    When Updating the repository using the maki file, with user not keeping source files that are not in the maki
    Then An HandlerError should not be raised
    And The "tmp/amaterasu/src" directory shouldn't have a "example.sql" file
    And The "tmp/amaterasu/src" directory shouldn't have a "example.R" file



  Scenario: Updating an existing repository with sources from a valid maki, and the sources in the repo are a superset of the ones in the maki, the user chooses to delete example.sql, only example.sql is deleted, example.R should stay
    Given The relative path "tmp/amaterasu"
    Given The path is a repository
    Given The "tmp/amaterasu" directory has a "src" subdirectory
    Given The "tmp/amaterasu" directory has a "env" subdirectory
    Given The "tmp/amaterasu/env" directory has a "default" subdirectory
    Given The "tmp/amaterasu" directory has a valid maki file
    Given The "tmp/amaterasu/src" directory has a "example.scala" file
    Given The "tmp/amaterasu/src" directory has a "example.py" file
    Given The "tmp/amaterasu/src" directory has a "example.sql" file
    Given The "tmp/amaterasu/src" directory has a "example.R" file
    When Updating the repository using the maki file, with user not keeping "example.sql" and is keeping "example.R"
    Then An HandlerError should not be raised
    And The "tmp/amaterasu/src" directory shouldn't have a "example.sql" file
    And The "tmp/amaterasu/src" directory should have a "example.R" file