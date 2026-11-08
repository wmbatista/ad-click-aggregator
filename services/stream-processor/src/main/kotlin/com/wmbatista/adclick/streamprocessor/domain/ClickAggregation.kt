package com.wmbatista.adclick.streamprocessor.domain

import java.time.temporal.ChronoUnit

/**
 * Counts [clicks] per ad and per minute (timestamp truncated to the minute; an [java.time.Instant]
 * is always UTC). The result has one entry per distinct pair, sorted by ad and then minute.
 */
fun aggregatePerMinute(clicks: List<Click>): List<ClickCount> =
    clicks
        .groupingBy { it.adId to it.timestamp.truncatedTo(ChronoUnit.MINUTES) }
        .eachCount()
        .map { (key, count) -> ClickCount(adId = key.first, minute = key.second, count = count.toLong()) }
        .sortedWith(compareBy({ it.adId }, { it.minute }))
