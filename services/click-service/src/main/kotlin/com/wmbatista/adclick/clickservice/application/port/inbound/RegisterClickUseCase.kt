package com.wmbatista.adclick.clickservice.application.port.inbound

import com.wmbatista.adclick.clickservice.domain.Click

/** Registers a click on an ad, making it durable for aggregation. */
interface RegisterClickUseCase {
    /**
     * Registers a click on [adId] and returns it once it has been durably published.
     *
     * @throws com.wmbatista.adclick.clickservice.application.port.outbound.ClickPublishingException
     *   if the click could not be published.
     */
    fun register(adId: String): Click
}
