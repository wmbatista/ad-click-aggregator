package com.wmbatista.adclick.clickservice.config

import org.springframework.boot.context.properties.ConfigurationProperties
import java.time.Duration

/**
 * @property topic the Kafka topic click events are published to; declared by this service.
 * @property sendTimeout how long a request waits for Kafka to acknowledge a click. Must not be
 *   shorter than the producer's `delivery.timeout.ms`.
 */
@ConfigurationProperties("click-service")
data class ClickServiceProperties(
    val topic: Topic,
    val sendTimeout: Duration,
) {
    data class Topic(
        val name: String,
        val partitions: Int,
        val replicationFactor: Short,
    )
}
