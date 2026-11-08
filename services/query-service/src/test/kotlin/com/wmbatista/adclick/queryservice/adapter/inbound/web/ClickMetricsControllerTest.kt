package com.wmbatista.adclick.queryservice.adapter.inbound.web

import com.wmbatista.adclick.queryservice.application.port.inbound.GetClickMetricsUseCase
import com.wmbatista.adclick.queryservice.application.port.outbound.ClickStoreUnavailableException
import com.wmbatista.adclick.queryservice.domain.ClickBucket
import com.wmbatista.adclick.queryservice.domain.ClickMetrics
import com.wmbatista.adclick.queryservice.domain.ClickQuery
import com.wmbatista.adclick.queryservice.domain.Granularity.MINUTE
import io.mockk.Called
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.springframework.test.json.JsonCompareMode
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.get
import org.springframework.test.web.servlet.setup.MockMvcBuilders
import java.time.Instant

class ClickMetricsControllerTest {

    private fun t(s: String) = Instant.parse(s)

    private val useCase = mockk<GetClickMetricsUseCase>()

    private val mockMvc: MockMvc = MockMvcBuilders
        .standaloneSetup(ClickMetricsController(useCase))
        .setControllerAdvice(ClickMetricsExceptionHandler())
        .build()

    private val specExample = ClickMetrics(
        adId = "ad-123",
        granularity = MINUTE,
        from = t("2026-11-08T10:00:00Z"),
        to = t("2026-11-08T10:03:00Z"),
        buckets = listOf(
            ClickBucket(t("2026-11-08T10:00:00Z"), 5),
            ClickBucket(t("2026-11-08T10:01:00Z"), 0),
            ClickBucket(t("2026-11-08T10:02:00Z"), 2),
        ),
    )

    @Test
    fun `returns the spec example`() {
        every { useCase.getClickMetrics(any()) } returns specExample

        mockMvc.get("/ads/ad-123/clicks?from=2026-11-08T10:00:00Z&to=2026-11-08T10:03:00Z&granularity=MINUTE")
            .andExpect {
                status { isOk() }
                content {
                    json(
                        """
                        {
                          "adId": "ad-123",
                          "granularity": "MINUTE",
                          "from": "2026-11-08T10:00:00Z",
                          "to": "2026-11-08T10:03:00Z",
                          "total": 7,
                          "buckets": [
                            { "start": "2026-11-08T10:00:00Z", "count": 5 },
                            { "start": "2026-11-08T10:01:00Z", "count": 0 },
                            { "start": "2026-11-08T10:02:00Z", "count": 2 }
                          ]
                        }
                        """,
                        JsonCompareMode.STRICT,
                    )
                }
            }
    }

    @Test
    fun `granularity defaults to MINUTE`() {
        val query = slot<ClickQuery>()
        every { useCase.getClickMetrics(capture(query)) } returns specExample

        mockMvc.get("/ads/ad-123/clicks?from=2026-11-08T10:00:00Z&to=2026-11-08T10:03:00Z")
            .andExpect { status { isOk() } }

        assertEquals(MINUTE, query.captured.granularity)
        assertEquals("ad-123", query.captured.adId)
    }

    @Test
    fun `accepts an instant with an offset`() {
        val query = slot<ClickQuery>()
        every { useCase.getClickMetrics(capture(query)) } returns specExample

        mockMvc.get("/ads/ad-123/clicks") {
            param("from", "2026-11-08T07:00:00-03:00")
            param("to", "2026-11-08T10:03:00Z")
        }.andExpect { status { isOk() } }

        assertEquals(t("2026-11-08T10:00:00Z"), query.captured.from)
    }

    @ParameterizedTest
    @ValueSource(
        strings = [
            "to=2026-11-08T10:03:00Z",
            "from=2026-11-08T10:00:00Z",
            "from=yesterday&to=2026-11-08T10:03:00Z",
            "from=2026-11-08T10:00:00Z&to=2026-11-08T10:03:00Z&granularity=WEEK",
            "from=2026-11-08T10:00:00Z&to=2026-11-08T10:03:00Z&granularity=minute",
            "from=2026-11-08T10:03:00Z&to=2026-11-08T10:00:00Z",
            "from=2026-11-08T00:00:00Z&to=2026-11-09T00:01:00Z",
        ],
    )
    fun `invalid parameters return 400 without calling the use case`(query: String) {
        mockMvc.get("/ads/ad-123/clicks?$query").andExpect { status { isBadRequest() } }

        verify { useCase wasNot Called }
    }

    @Test
    fun `validation errors explain the reason`() {
        mockMvc.get("/ads/ad-123/clicks?from=2026-11-08T10:03:00Z&to=2026-11-08T10:00:00Z").andExpect {
            status { isBadRequest() }
            jsonPath("$.detail") { exists() }
        }
    }

    @Test
    fun `store unavailable returns 503`() {
        every { useCase.getClickMetrics(any()) } throws ClickStoreUnavailableException("down")

        mockMvc.get("/ads/ad-123/clicks?from=2026-11-08T10:00:00Z&to=2026-11-08T10:03:00Z")
            .andExpect { status { isServiceUnavailable() } }
    }
}
