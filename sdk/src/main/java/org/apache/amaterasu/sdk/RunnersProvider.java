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
package org.apache.amaterasu.sdk;

import org.apache.amaterasu.common.dataobjects.ExecData;
import org.apache.amaterasu.common.execution.actions.Notifier;

import java.io.ByteArrayOutputStream;

/**
 * RunnersProvider an interface representing a factory that creates a group of related
 * runners. For example the SparkProvider is a factory for all the spark runners
 * (Scala, Python, R, SQL, etc.)
 */
public interface RunnersProvider {

    void init(ExecData data,
              String jobId,
              ByteArrayOutputStream outStream,
              Notifier notifier,
              String executorId,
              String propFile);

    String getGroupIdentifier();

    AmaterasuRunner getRunner(String id);
}