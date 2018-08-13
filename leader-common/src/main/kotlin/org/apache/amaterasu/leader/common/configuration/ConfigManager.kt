package org.apache.amaterasu.leader.common.configuration

import com.uchuhimo.konf.Config
import java.io.File

class ConfigManager(env: String, repoPath: String) {

    private val envFolder = "$repoPath/$env"
    val config = Config{
        addSpec(Job)
    }

    init {
        println("++++++++++++++++++++++++++++")
        println(envFolder)
        println("++++++++++++++++++++++++++++")
        for(file in File(envFolder).listFiles()){
            config.from.yaml.file(file)
        }
    }

//    fun getConfigurationWriter():  = {
//        config.toYaml.toWrite
//    }
}