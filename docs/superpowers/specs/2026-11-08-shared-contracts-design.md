# Shared Contracts — Stream Processor, Query Service and Local Infrastructure

**Date:** 2026-11-08
**Status:** Approved

## Intent

Three pieces of work are about to start, possibly in parallel and in separate sessions or worktrees:

- **A — infrastructure:** Docker Compose with Kafka, ClickHouse and the three services, plus the
  bootstrap script.
- **B — `services/stream-processor`:** consumes `click-events`, aggregates per ad and minute, writes
  to ClickHouse.
- **C — `services/query-service`:** serves click metrics from ClickHouse over HTTP.

They share a table, configuration names, ports, images and dependency versions. This document fixes
those contracts so each piece can be built without coordinating with the others. Anything not
listed here is left to the builder of each piece.

## Decided by the user

- **Idempotency is out of scope for now.** The pipeline is at-least-once and may over-count after
  replays. Tracked as [TD-1 in `docs/tech-debt.md`](../../tech-debt.md). No piece of work should add
  deduplication logic or design around it yet.

## Calls made by Claude (open to review)

### 1. ClickHouse schema

Owned by **A** (it lives in the init scripts mounted by Compose). **B** writes it, **C** reads it,
and both use the same DDL in their Testcontainers tests.

File: `infra/clickhouse/init/001-schema.sql`

```sql
CREATE DATABASE IF NOT EXISTS ad_clicks;

CREATE TABLE IF NOT EXISTS ad_clicks.clicks_per_minute
(
    ad_id       String,
    minute      DateTime('UTC'),   -- start of the 1-minute bucket
    click_count UInt64
)
ENGINE = SummingMergeTree(click_count)
PARTITION BY toYYYYMMDD(minute)
ORDER BY (ad_id, minute);
```

Rules that follow from the engine:

- **Writers only insert.** Every batch inserts *delta* rows `(ad_id, minute, n)`. There is no
  `UPDATE`; `SummingMergeTree` adds rows with the same `(ad_id, minute)` together when parts merge.
  This is the "upsert, adding to the existing count" from the diagram. Late clicks for an old
  minute are just another delta row.
- **Readers always aggregate.** Merges are asynchronous, so one `(ad_id, minute)` may exist as
  several rows at any moment. Every query must use `sum(click_count)` with `GROUP BY`; reading
  `click_count` directly is a bug.
- Column `click_count` instead of `count` (from `architecture.md`) to avoid confusion with the
  `count()` function in queries. Column names are `snake_case`; Kotlin code maps them.
- The minute is computed by the writer: `timestamp` of the `ClickEvent` truncated to the minute in
  UTC.

### 2. Kafka topic

- Topic `click-events`, 12 partitions, replication factor 1, key = `adId`, value = `ClickEvent`
  JSON without type headers (unchanged from the click service).
- **A creates the topic** with a one-shot init container in Compose, so it exists before any
  consumer starts. Broker auto-creation of topics is disabled.
- The click service keeps its `NewTopic` bean as a fallback for running it outside Compose; it is a
  no-op when the topic already exists. Its partition and replication values must stay equal to the
  Compose ones.

### 3. Stream processor (B)

- Spring Boot, hexagonal layout, same conventions as `click-service` (the module switches to
  `kotlin-spring-service-convention`). Package `com.wmbatista.adclick.streamprocessor`.
- Consumer group `stream-processor`, `auto.offset.reset=earliest`, auto commit disabled.
- Batch listener: one Kafka poll (`max.poll.records=500`) is one batch. For each batch:
  aggregate in memory by `(adId, minute)`, insert the rows into ClickHouse in a single insert, and
  only then commit the offsets. **Offsets are never committed before the insert succeeded.**
- If the insert fails, the batch is retried with backoff and the offsets are not committed.
  A record that cannot be deserialized is logged and skipped (it can never succeed).
- Deserializes `ClickEvent` from `libs/click-event`, ignoring type headers.
- Exposes only Actuator health (no business HTTP API).

### 4. Query service (C)

- Spring Boot, hexagonal layout, same conventions as `click-service`. Package
  `com.wmbatista.adclick.queryservice`.
- Endpoint:

  ```
  GET /ads/{adId}/clicks?from=<instant>&to=<instant>&granularity=<MINUTE|HOUR|DAY>
  ```

  - `from` (inclusive) and `to` (exclusive): ISO-8601 UTC instants, both required, `from < to`.
  - `granularity`: optional, default `MINUTE`. `from` is truncated down and `to` rounded up to the
    granularity.
  - At most **1,440 buckets** per request (one day at minute granularity); more returns `400`.
  - Response `200`:

    ```json
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
    ```

  - Buckets are **zero-filled**: every bucket in the range appears, in order, so a chart can plot it
    directly. An unknown `adId` is not an error; it returns all zeros.
  - Invalid or missing parameters: `400`. ClickHouse unavailable: `503`.
- Reads with `sum(click_count)` grouped by the bucket (`toStartOfMinute/Hour/Day`), as required by
  the schema rules above.

### 5. Configuration, ports and hostnames

Every service reads its connection settings from environment variables with a default that works
when running on the host against the Compose infrastructure.

| Variable                  | Used by                 | Default (host)                            | Value inside Compose                       |
|---------------------------|-------------------------|-------------------------------------------|--------------------------------------------|
| `KAFKA_BOOTSTRAP_SERVERS` | click, stream-processor | `localhost:9092`                          | `kafka:29092`                              |
| `CLICKHOUSE_URL`          | stream-processor, query | `jdbc:clickhouse://localhost:8123/ad_clicks` | `jdbc:clickhouse://clickhouse:8123/ad_clicks` |
| `CLICKHOUSE_USER`         | stream-processor, query | `ad_clicks`                               | `ad_clicks`                                |
| `CLICKHOUSE_PASSWORD`     | stream-processor, query | `ad_clicks`                               | `ad_clicks`                                |

| Component        | Port on the host | Notes                                        |
|------------------|------------------|----------------------------------------------|
| click-service    | 8080             | `POST /click`                                |
| query-service    | 8081             | `GET /ads/{adId}/clicks`                     |
| stream-processor | 8082             | Actuator health only                         |
| Kafka            | 9092             | external listener; `kafka:29092` internally  |
| ClickHouse       | 8123 (HTTP), 9000 (native) |                                    |

Every service exposes `GET /actuator/health`, which Compose uses as its healthcheck.

### 6. Images and versions

- **Infrastructure images** (used both by Compose and by Testcontainers, so tests run against the
  same versions):
  - Kafka: `apache/kafka:4.2.2` (KRaft, single node; matches the Kafka client 4.2.2 managed by
    Spring Boot 4.1.2).
  - ClickHouse: `clickhouse/clickhouse-server:26.8.36.3` (latest LTS line).
- **Service images:** the jar is built on the host with `./gradlew bootJar`; a single shared
  `infra/docker/service.Dockerfile` (`eclipse-temurin:25-jre` base, jar passed as a build arg) packages
  any of the three services. The bootstrap script runs the Gradle build and then
  `docker compose up --build`. Chosen over building inside Docker (slow, needs a Gradle cache in the
  image) and over `bootBuildImage` (heavy buildpack downloads).
- **New version catalog entries**, added once before the parallel work starts so that B and C do not
  edit `gradle/libs.versions.toml` concurrently:
  - `com.clickhouse:clickhouse-jdbc:0.11.0` (driver, used through Spring `JdbcTemplate`)
  - `spring-boot-starter-jdbc` and `spring-boot-starter-actuator` (BOM-managed)
  - `spring-boot-testcontainers`, `org.testcontainers:testcontainers-junit-jupiter`,
    `testcontainers-clickhouse`, `testcontainers-kafka` (Testcontainers 2.0.5, BOM-managed)

## Ownership and file boundaries

| Work | Owns (may edit) | Must not edit |
|------|-----------------|---------------|
| A | `infra/`, `scripts/`, `services/click-service` only to expose Actuator health | `services/stream-processor`, `services/query-service`, version catalog |
| B | `services/stream-processor/` | everything else |
| C | `services/query-service/` | everything else |

Already done before A, B and C start (Phase 0): this document, `docs/tech-debt.md`, the schema file
`infra/clickhouse/init/001-schema.sql`, and the version catalog entries above.

Docs (`README.md`, `docs/architecture.md`) are updated in the integration step after A, B and C are
merged, not by each piece, to avoid conflicts.

## Non-goals

- Idempotency and deduplication (TD-1).
- Click redirect, authentication, rate limiting, TLS, multi-broker Kafka, ClickHouse replication.
- Simulation scripts and validation guides: they come in the integration step, once everything runs.

## Left to each builder

Internal package structure beyond the hexagonal layers, class names, test split between unit and
Testcontainers tests, retry backoff values, logging, Compose healthcheck timings, and the exact way
the schema file is loaded in tests.
