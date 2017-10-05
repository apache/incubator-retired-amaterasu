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
package org.apache.amaterasu.leader.yarn;

import org.apache.commons.cli.*;

public class ArgsParser {
    private static Options getOptions() {

        Options options = new Options();
        options.addOption("r", "repo", true, "The git repo containing the job");
        options.addOption("b", "branch", true, "The branch to be executed (default is master)");
        options.addOption("e", "env", true, "The environment to be executed (test, prod, etc. values from the default env are taken if np env specified)");
        options.addOption("n", "name", true, "The name of the job");
        options.addOption("i", "job-id", true, "The jobId - should be passed only when resuming a job");
        options.addOption("j", "new-job-id", true, "The jobId - should never be passed by a user");
        options.addOption("r", "report", true, "The level of reporting");
        options.addOption("h", "home", true, "The level of reporting");
        options.addOption("c", "config-home", true, "Path to where the Amaterasu configuration resides. Usually it should be at ~/.amaterasu/");

        return options;
    }

    public static JobOpts getJobOpts(String[] args) throws ParseException {

        CommandLineParser parser = new BasicParser();
        Options options = getOptions();
        CommandLine cli = parser.parse(options, args);

        JobOpts opts = new JobOpts();
        if (cli.hasOption("repo")) {
            opts.repo = cli.getOptionValue("repo");
        }

        if (cli.hasOption("branch")) {
            opts.branch = cli.getOptionValue("branch");
        }

        if (cli.hasOption("env")) {
            opts.env = cli.getOptionValue("env");
        }

        if (cli.hasOption("job-id")) {
            opts.jobId = cli.getOptionValue("job-id");
        }
        if (cli.hasOption("new-job-id")) {
            opts.newJobId = cli.getOptionValue("new-job-id");
        }

        if (cli.hasOption("report")) {
            opts.report = cli.getOptionValue("report");
        }

        if (cli.hasOption("home")) {
            opts.home = cli.getOptionValue("home");
        }

        if (cli.hasOption("name")) {
            opts.name = cli.getOptionValue("name");
        }

        if (cli.hasOption("config-home")) {
            opts.configHome = cli.getOptionValue("config-home");
        }

        return opts;
    }
}
