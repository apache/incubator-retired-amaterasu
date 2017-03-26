package io.shinto.amaterasu.executor.execution.actions.runners.spark.PySpark;

import io.shinto.amaterasu.executor.runtime.AmaContext;
import io.shinto.amaterasu.common.runtime.Environment;

import org.apache.spark.SparkEnv;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;

import py4j.GatewayServer;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.concurrent.ConcurrentHashMap;

public class PySparkEntryPoint {

    private static Boolean started = false;
    private static PySparkExecutionQueue queue = new PySparkExecutionQueue();
    private static ConcurrentHashMap<String, ResultQueue> resultQueues = new ConcurrentHashMap<>();

    private static int port = 0;
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

    public static SQLContext getSqlContext() {
        SparkEnv.set(sparkEnv);
        return sqlContext;
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

    public static void start(SparkContext sc,
                             String jobName,
                             Environment env,
                             SparkEnv sparkEnv) {

        if (!started) {
            AmaContext.init(sc, new SQLContext(sc), jobName, env);
            started = true;
        }

        jsc = new JavaSparkContext(sc);
        sqlContext = new SQLContext(sc);
        PySparkEntryPoint.sparkEnv = sparkEnv;
        generatePort();
        GatewayServer gatewayServer = new GatewayServer(new PySparkEntryPoint(), port);

        gatewayServer.start();
    }
}
