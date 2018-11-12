package org.apache.amaterasu.common.logging;

import org.slf4j.Logger;

/**
 * Created by Eran Bartenstein (p765790) on 5/11/18.
 */
public abstract class Logging extends KLogging {
    protected Logger log = getLog();
}
