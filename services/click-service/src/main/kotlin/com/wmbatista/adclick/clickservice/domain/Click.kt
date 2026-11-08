package com.wmbatista.adclick.clickservice.domain

import java.time.Instant
import java.util.UUID

/** An ad click registered by the service. */
data class Click(
    val clickId: UUID,
    val adId: String,
    val timestamp: Instant,
)
