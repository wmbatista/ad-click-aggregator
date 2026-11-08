package com.wmbatista.adclick.streamprocessor.domain

import java.time.Instant

/**
 * How many clicks an ad received in one minute, within one batch. It is a delta: counts for the
 * same ad and minute from other batches are added to it by the store.
 *
 * @property minute start of the 1-minute bucket, always a whole minute in UTC.
 */
data class ClickCount(
    val adId: String,
    val minute: Instant,
    val count: Long,
)
