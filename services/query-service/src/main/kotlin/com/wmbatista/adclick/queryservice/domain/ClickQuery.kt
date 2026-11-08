package com.wmbatista.adclick.queryservice.domain

import java.time.Duration
import java.time.Instant

/**
 * A validated request for the clicks on [adId] over `[from, to)`. [from] and [to] are already aligned
 * to [granularity]: `from` truncated down, `to` rounded up. Build it with [of].
 */
class ClickQuery private constructor(
    val adId: String,
    val from: Instant,
    val to: Instant,
    val granularity: Granularity,
) {
    /** Start of every bucket in the range, in order. */
    val bucketStarts: List<Instant>
        get() = generateSequence(from) { it.plus(1, granularity.unit) }.takeWhile { it < to }.toList()

    companion object {
        /** One day at minute granularity. */
        const val MAX_BUCKETS = 1440

        /** Range of ClickHouse `DateTime`, rounded in to whole days so every granularity stays inside it. */
        private val EARLIEST = Instant.parse("1970-01-01T00:00:00Z")
        private val LATEST = Instant.parse("2106-01-01T00:00:00Z")

        /** @throws InvalidClickQueryException if the query is empty, too large or outside the storable range. */
        fun of(adId: String, from: Instant, to: Instant, granularity: Granularity): ClickQuery {
            if (adId.isBlank()) throw InvalidClickQueryException("adId must not be blank")
            if (from >= to) throw InvalidClickQueryException("from ($from) must be before to ($to)")
            // Checked before aligning too: rounding up an Instant near Instant.MAX would overflow.
            if (from < EARLIEST || to > LATEST) throw outOfRange()

            val unit = granularity.unit
            val alignedFrom = from.truncatedTo(unit)
            val truncatedTo = to.truncatedTo(unit)
            val alignedTo = if (truncatedTo == to) to else truncatedTo.plus(1, unit)

            if (alignedFrom < EARLIEST || alignedTo > LATEST) throw outOfRange()
            val buckets = Duration.between(alignedFrom, alignedTo).dividedBy(unit.duration)
            if (buckets > MAX_BUCKETS) {
                throw InvalidClickQueryException(
                    "range covers $buckets ${granularity.name} buckets; at most $MAX_BUCKETS are allowed",
                )
            }
            return ClickQuery(adId, alignedFrom, alignedTo, granularity)
        }

        private fun outOfRange() = InvalidClickQueryException("from and to must be between $EARLIEST and $LATEST")
    }
}
