package com.wmbatista.adclick.streamprocessor.adapter.inbound.kafka

import com.wmbatista.adclick.event.ClickEvent
import com.wmbatista.adclick.streamprocessor.application.port.inbound.AggregateClicksUseCase
import com.wmbatista.adclick.streamprocessor.domain.Click
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.commons.logging.LogFactory
import org.springframework.core.log.LogAccessor
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.serializer.SerializationUtils

/**
 * Consumes `click-events` one poll at a time. The container commits a poll's offsets only when
 * [onBatch] returns normally, so a failure to store the counts leaves the batch to be retried.
 */
class ClickEventsListener(
    private val aggregateClicks: AggregateClicksUseCase,
) {

    @KafkaListener(topics = ["\${stream-processor.topic}"])
    fun onBatch(records: List<ConsumerRecord<String, ClickEvent?>>) {
        aggregateClicks.aggregate(records.mapNotNull(::toClick))
    }

    /** Records that can never be counted are logged and skipped; retrying them would not help. */
    private fun toClick(record: ConsumerRecord<String, ClickEvent?>): Click? {
        val event = record.value()
        return when {
            event == null -> {
                val cause = SerializationUtils.getExceptionFromHeader(
                    record,
                    SerializationUtils.VALUE_DESERIALIZER_EXCEPTION_HEADER,
                    logger,
                )
                if (cause != null) {
                    logger.warn(cause) { "Skipping ${record.position()}: value is not a ClickEvent" }
                } else {
                    logger.warn { "Skipping ${record.position()}: null value" }
                }
                null
            }
            event.adId.isBlank() -> {
                logger.warn { "Skipping ${record.position()}: blank adId" }
                null
            }
            else -> Click(adId = event.adId, timestamp = event.timestamp)
        }
    }

    private fun ConsumerRecord<*, *>.position() = "${topic()}-${partition()}@${offset()}"

    private companion object {
        val logger = LogAccessor(LogFactory.getLog(ClickEventsListener::class.java))
    }
}
