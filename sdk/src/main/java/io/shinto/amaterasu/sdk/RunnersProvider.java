package io.shinto.amaterasu.sdk;

import io.shinto.amaterasu.common.dataobjects.ExecData;
import io.shinto.amaterasu.common.execution.actions.Notifier;
import io.shinto.amaterasu.common.runtime.Environment;

import java.io.ByteArrayOutputStream;
import java.util.List;

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
              String executorId);
    String getGroupIdentifier();

    AmaterasuRunner getRunner(String id);
}