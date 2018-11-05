package org.apache.amaterasu.common.logging

import org.slf4j.LoggerFactory

/**
 * Created by Eran Bartenstein on 5/11/18.
 */
abstract class KLogging {
    protected var log = LoggerFactory.getLogger(this.javaClass.name)
}
