# Technical Debt

Known gaps that we accepted on purpose to move forward. Each entry says what is missing, why it
was deferred, what it breaks, and the options for paying it back. Remove an entry once it is paid.

## TD-1: Click counting is not idempotent

**Recorded:** 2026-11-08
**Requirement affected:** _Idempotency: the same click must not be counted more than once_
(see [`architecture.md`](architecture.md)).

### What is missing

The pipeline is at-least-once end to end, and nothing downstream deduplicates:

1. **Aggregator replays.** `stream-processor` inserts a batch's per-minute counts into ClickHouse
   and only then commits its Kafka offsets. If it crashes (or the commit fails, or a rebalance
   happens) between the insert and the commit, the batch is consumed again and its counts are
   inserted again. The `SummingMergeTree` table adds them up, so those clicks are counted twice.
2. **Insert retries.** A ClickHouse insert that times out on the client but succeeded on the server
   is retried, with the same effect.
3. **Click service `503` window.** A click answered with `503` may still be written by Kafka (see
   the click service section of `architecture.md`). The browser's retry then produces a second
   event with a new `clickId`, which no downstream stage can tell apart from a genuine click.

The `clickId` carried by every `ClickEvent` is not used by anyone yet.

### Why it was deferred

The first goal is a working end-to-end pipeline (click → Kafka → aggregator → ClickHouse → query).
Idempotency touches the table design, the aggregator's batching and the click API, so it is worth
designing on its own once the pipeline runs and the double count can be reproduced.

### Impact today

Counts can be **over**-reported after aggregator crashes, restarts during a batch, consumer group
rebalances, or ClickHouse timeouts. Clicks are never lost: under-counting is still prevented by
committing offsets only after the insert.

### Options for paying it back

- **Offset-keyed rows.** Write each batch's rows with the Kafka `(partition, first offset)` of the
  batch in the key and use `ReplacingMergeTree`, so a replayed batch replaces instead of adds.
  Requires batch boundaries that are deterministic across replays (e.g. cut batches at fixed offset
  ranges per partition).
- **ClickHouse insert deduplication.** Send `insert_deduplication_token` derived from the batch's
  partition and offset range (needs `non_replicated_deduplication_window` on a non-replicated
  table). Same deterministic-batch requirement as above.
- **Raw clicks plus materialized view.** Store raw clicks in a `ReplacingMergeTree` ordered by
  `clickId` and aggregate in ClickHouse. Deduplicates by `clickId`, but merges are eventual, so
  queries need `FINAL` or tolerate temporary duplicates; moves aggregation out of the aggregator.
- **Exactly-once stream processor.** Replace the plain consumer with Apache Flink (checkpointing
  plus a transactional or idempotent sink), as the diagram suggests.
- **Client idempotency key** (for item 3 only). The caller sends a key per click (e.g. a signed
  impression id) that becomes the `clickId`, so a browser retry carries the same id.
