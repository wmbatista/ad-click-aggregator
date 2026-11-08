package com.wmbatista.adclick.event

import java.time.Instant
import java.util.UUID

/**
 * Wire contract of the `click-events` Kafka topic, shared by the click service (producer) and the
 * stream processor (consumer). Messages are keyed by [adId] and serialized as JSON, with
 * [timestamp] as an ISO-8601 UTC instant and [clickId] as a UUID string.
 *
 * @property clickId unique identifier of the click, used to deduplicate re-deliveries.
 * @property adId the ad that was clicked.
 * @property timestamp when the click service received the click.
 */
data class ClickEvent(
    val clickId: UUID,
    val adId: String,
    val timestamp: Instant,
)
