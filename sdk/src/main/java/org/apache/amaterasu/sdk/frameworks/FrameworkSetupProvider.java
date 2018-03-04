package org.apache.amaterasu.sdk.frameworks;

import org.apache.amaterasu.common.configuration.ClusterConfig;
import org.apache.amaterasu.sdk.frameworks.configuration.DriverConfiguration;

import java.io.File;

public interface FrameworkSetupProvider {

    void init(ClusterConfig conf);

    String getGroupIdentifier();

    File[] getGroupResources();

    File[] getRunnerResources(String runnerId);

    DriverConfiguration getDriverConfiguration();

}