package org.apache.amaterasu.common.configuration.enums

/**
 * Created by Eran Bartenstein on 21/10/18.
 */
enum class ActionStatus (val value: String) {
    pending("pending"),
    queued("queued"),
    started("started"),
    complete("complete"),
    failed("failed"),
    canceled("canceled")
}
