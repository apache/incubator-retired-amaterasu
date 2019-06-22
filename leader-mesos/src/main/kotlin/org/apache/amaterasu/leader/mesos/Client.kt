package org.apache.amaterasu.leader.mesos

object Client {

    @Throws(Exception::class)
    @JvmStatic
    fun main(args: Array<String>) = ClientArgsParser().main(args)
}