package com.wmbatista.adclick.queryservice.domain

import java.time.temporal.ChronoUnit

/** Size of the buckets a click query is grouped into. All buckets are aligned to UTC. */
enum class Granularity(val unit: ChronoUnit) {
    MINUTE(ChronoUnit.MINUTES),
    HOUR(ChronoUnit.HOURS),
    DAY(ChronoUnit.DAYS),
}
