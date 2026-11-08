package com.wmbatista.adclick.streamprocessor.config

import org.springframework.boot.context.properties.ConfigurationProperties
import java.time.Duration

/**
 * @property topic the Kafka topic click events are consumed from. Created by the infrastructure,
 *   never by this service.
 * @property retry backoff between retries of a batch whose ClickHouse insert failed.
 */
@ConfigurationProperties("stream-processor")
data class StreamProcessorProperties(
    val topic: String,
    val retry: Retry,
) {
    /**
     * Exponential backoff with no attempt limit: the delay starts at [initialInterval], grows by
     * [multiplier] and is capped at [maxInterval].
     */
    data class Retry(
        val initialInterval: Duration,
        val multiplier: Double,
        val maxInterval: Duration,
    )
}
