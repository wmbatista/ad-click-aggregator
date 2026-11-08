package com.wmbatista.adclick.clickservice.adapter.outbound.kafka

import com.wmbatista.adclick.clickservice.application.port.outbound.ClickPublishingException
import com.wmbatista.adclick.clickservice.application.port.outbound.PublishClickPort
import com.wmbatista.adclick.clickservice.domain.Click
import com.wmbatista.adclick.event.ClickEvent
import org.apache.kafka.common.KafkaException
import org.springframework.kafka.core.KafkaTemplate
import java.time.Duration
import org.springframework.kafka.KafkaException as SpringKafkaException
import java.util.concurrent.ExecutionException
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

/**
 * Publishes clicks to Kafka keyed by `adId`, blocking until the broker acknowledges the write or
 * [sendTimeout] elapses.
 */
class KafkaClickPublisher(
    private val kafkaTemplate: KafkaTemplate<String, ClickEvent>,
    private val topic: String,
    private val sendTimeout: Duration,
) : PublishClickPort {

    override fun publish(click: Click) {
        try {
            kafkaTemplate.send(topic, click.adId, click.toClickEvent())
                .get(sendTimeout.toMillis(), TimeUnit.MILLISECONDS)
        } catch (e: InterruptedException) {
            Thread.currentThread().interrupt()
            throw ClickPublishingException("Interrupted while publishing click ${click.clickId}", e)
        } catch (e: ExecutionException) {
            throw ClickPublishingException("Kafka rejected click ${click.clickId}", e.cause ?: e)
        } catch (e: TimeoutException) {
            throw ClickPublishingException("Timed out after $sendTimeout publishing click ${click.clickId}", e)
        } catch (e: KafkaException) {
            throw ClickPublishingException("Failed to send click ${click.clickId}", e)
        } catch (e: SpringKafkaException) {
            // KafkaTemplate wraps synchronous send failures, e.g. no broker metadata within max.block.ms.
            throw ClickPublishingException("Failed to send click ${click.clickId}", e)
        }
    }
}
