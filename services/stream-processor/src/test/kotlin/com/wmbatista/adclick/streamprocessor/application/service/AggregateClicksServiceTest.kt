package com.wmbatista.adclick.streamprocessor.application.service

import com.wmbatista.adclick.streamprocessor.application.port.outbound.SaveClickCountsPort
import com.wmbatista.adclick.streamprocessor.domain.Click
import com.wmbatista.adclick.streamprocessor.domain.ClickCount
import io.mockk.Called
import io.mockk.every
import io.mockk.justRun
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.time.Instant

class AggregateClicksServiceTest {

    private val port = mockk<SaveClickCountsPort>()
    private val service = AggregateClicksService(port)

    @Test
    fun `saves the per-minute counts once`() {
        justRun { port.save(any()) }

        service.aggregate(
            listOf(
                Click("ad-1", Instant.parse("2026-11-08T10:00:05Z")),
                Click("ad-1", Instant.parse("2026-11-08T10:00:45Z")),
            ),
        )

        verify(exactly = 1) { port.save(listOf(ClickCount("ad-1", Instant.parse("2026-11-08T10:00:00Z"), 2))) }
    }

    @Test
    fun `empty batch does not touch the port`() {
        service.aggregate(emptyList())

        verify { port wasNot Called }
    }

    @Test
    fun `port failure propagates`() {
        every { port.save(any()) } throws IllegalStateException("down")

        assertThrows<IllegalStateException> {
            service.aggregate(listOf(Click("ad-1", Instant.parse("2026-11-08T10:00:05Z"))))
        }
    }
}
