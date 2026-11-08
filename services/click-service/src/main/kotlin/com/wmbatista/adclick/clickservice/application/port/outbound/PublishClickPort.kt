package com.wmbatista.adclick.clickservice.application.port.outbound

import com.wmbatista.adclick.clickservice.domain.Click

/** Publishes registered clicks for downstream aggregation. */
fun interface PublishClickPort {
    /**
     * Publishes [click], returning only once it has been durably accepted.
     *
     * @throws ClickPublishingException if the click could not be published.
     */
    fun publish(click: Click)
}
