package com.wmbatista.adclick.streamprocessor.adapter.inbound.kafka

import com.wmbatista.adclick.event.ClickEvent
import com.wmbatista.adclick.streamprocessor.application.port.inbound.AggregateClicksUseCase
import com.wmbatista.adclick.streamprocessor.domain.Click
import io.mockk.every
import io.mockk.justRun
import io.mockk.mockk
import io.mockk.verify
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.time.Instant
import java.util.UUID

class ClickEventsListenerTest {

    private val useCase = mockk<AggregateClicksUseCase>()
    private val listener = ClickEventsListener(useCase)

    @Test
    fun `maps every valid record to a click in one call`() {
        justRun { useCase.aggregate(any()) }

        listener.onBatch(listOf(record(0, event("ad-1", "10:00:05")), record(1, event("ad-2", "10:00:07"))))

        verify(exactly = 1) {
            useCase.aggregate(listOf(Click("ad-1", at("10:00:05")), Click("ad-2", at("10:00:07"))))
        }
    }

    @Test
    fun `null values and blank ad ids are skipped`() {
        justRun { useCase.aggregate(any()) }

        listener.onBatch(listOf(record(0, event("ad-1", "10:00:05")), record(1, null), record(2, event("  ", "10:00:06"))))

        verify(exactly = 1) { useCase.aggregate(listOf(Click("ad-1", at("10:00:05")))) }
    }

    @Test
    fun `batch with only bad records aggregates nothing and does not throw`() {
        justRun { useCase.aggregate(any()) }

        listener.onBatch(listOf(record(0, null), record(1, null)))

        verify(exactly = 1) { useCase.aggregate(emptyList()) }
    }

    @Test
    fun `use case failure propagates`() {
        every { useCase.aggregate(any()) } throws RuntimeException("insert failed")

        assertThrows<RuntimeException> { listener.onBatch(listOf(record(0, event("ad-1", "10:00:05")))) }
    }

    private fun record(offset: Long, value: ClickEvent?) =
        ConsumerRecord<String, ClickEvent?>("click-events", 0, offset, value?.adId, value)

    private fun event(adId: String, time: String) = ClickEvent(UUID.randomUUID(), adId, at(time))

    private fun at(time: String): Instant = Instant.parse("2026-11-08T${time}Z")
}
