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
package org.apache.amaterasu.executor.execution.actions.runners.spark.PySpark;

import org.apache.amaterasu.executor.runtime.AmaContext;
import org.apache.amaterasu.common.runtime.Environment;

import org.apache.spark.SparkEnv;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;

import org.apache.spark.sql.SparkSession;
import py4j.GatewayServer;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.concurrent.ConcurrentHashMap;

public class PySparkEntryPoint {

    //private static Boolean started = false;
    private static PySparkExecutionQueue queue = new PySparkExecutionQueue();
    private static ConcurrentHashMap<String, ResultQueue> resultQueues = new ConcurrentHashMap<>();

    private static int port = 0;
    private static SparkSession sparkSession = null;
    private static JavaSparkContext jsc = null;
    private static SQLContext sqlContext = null;
    private static SparkEnv sparkEnv = null;

    public static PySparkExecutionQueue getExecutionQueue() {
        return queue;
    }

    public static ResultQueue getResultQueue(String actionName) {
        resultQueues.putIfAbsent(actionName, new ResultQueue());
        return resultQueues.get(actionName);
    }

    public static JavaSparkContext getJavaSparkContext() {
        SparkEnv.set(sparkEnv);
        return jsc;
    }

    public static String getJobId(){
        return AmaContext.jobId();
    }

    public static Environment getEnv(){
        return AmaContext.env();
    }

    public static SQLContext getSqlContext() {
        SparkEnv.set(sparkEnv);
        return sqlContext;
    }

    public static SparkSession getSparkSession() {
        SparkEnv.set(sparkEnv);
        return sparkSession;
    }

    public static SparkConf getSparkConf() {
        return jsc.getConf();
    }

    private static void generatePort() {

        try {

            ServerSocket socket = new ServerSocket(0);
            port = socket.getLocalPort();

            socket.close();

        } catch (IOException e) {
        }

    }

    public static int getPort() {
        return port;
    }

    public static void start(SparkSession spark,
                             String jobName,
                             Environment env,
                             SparkEnv sparkEnv) {

        AmaContext.init(spark, jobName, env);

        sparkSession = spark;
        jsc = new JavaSparkContext(spark.sparkContext());
        sqlContext = spark.sqlContext();
        PySparkEntryPoint.sparkEnv = sparkEnv;
        generatePort();
        GatewayServer gatewayServer = new GatewayServer(new PySparkEntryPoint(), port);

        gatewayServer.start();
    }
}
