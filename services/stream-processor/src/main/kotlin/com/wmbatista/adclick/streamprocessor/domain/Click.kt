package com.wmbatista.adclick.streamprocessor.domain

import java.time.Instant

/**
 * A click to be counted.
 *
 * @property adId the ad that was clicked.
 * @property timestamp when the click service received the click.
 */
data class Click(
    val adId: String,
    val timestamp: Instant,
)
