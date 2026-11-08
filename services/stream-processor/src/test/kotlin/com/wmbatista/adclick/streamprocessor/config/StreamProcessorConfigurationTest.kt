package com.wmbatista.adclick.streamprocessor.config

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Test
import org.springframework.util.backoff.BackOffExecution
import java.time.Duration

class StreamProcessorConfigurationTest {

    private val configuration = StreamProcessorConfiguration()
    private val properties = StreamProcessorProperties(
        topic = "click-events",
        retry = StreamProcessorProperties.Retry(
            initialInterval = Duration.ofMillis(500),
            multiplier = 2.0,
            maxInterval = Duration.ofSeconds(30),
        ),
    )

    @Test
    fun `batch backoff never gives up`() {
        // Giving up would let the error handler skip the batch and commit its offsets.
        val execution = configuration.clickBatchBackOff(properties).start()

        repeat(10_000) { assertNotEquals(BackOffExecution.STOP, execution.nextBackOff()) }
    }

    @Test
    fun `batch backoff is capped at max interval`() {
        val execution = configuration.clickBatchBackOff(properties).start()

        assertEquals(500L, execution.nextBackOff())
        repeat(19) { execution.nextBackOff() }
        assertEquals(30_000L, execution.nextBackOff())
    }
}
