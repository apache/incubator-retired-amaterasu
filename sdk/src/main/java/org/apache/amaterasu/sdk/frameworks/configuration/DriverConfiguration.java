package org.apache.amaterasu.sdk.frameworks.configuration;

public class DriverConfiguration {


    private int memory = 0;
    private int cpus = 0;

    public DriverConfiguration(int memory, int cpus) {
        this.memory = memory;
        this.cpus = cpus;
    }

    public int getMemory() {
        return memory;
    }

    public int getCPUs() {
        return cpus;
    }
}
