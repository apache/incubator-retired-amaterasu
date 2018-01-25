package org.apache.amaterasu.sdk;

import org.apache.amaterasu.common.configuration.ClusterConfig;

import java.io.File;

public interface FrameworkSetupProvider {

    void init(ClusterConfig conf);

    String getGroupIdentifier();

    File[] getGroupResources();

    File[] getRunnerResources(String runnerId);

}