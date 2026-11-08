# Stream Processor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn the `services/stream-processor` placeholder into a Spring Boot service that consumes
`click-events` in batches, aggregates clicks per `(adId, minute)` and inserts the deltas into
ClickHouse, committing Kafka offsets only after the insert succeeded.

**Architecture:** Hexagonal, like `click-service`. Domain (`Click`, `ClickCount`,
`aggregatePerMinute`) and application layer (inbound port `AggregateClicksUseCase`, outbound port
`SaveClickCountsPort`, service `AggregateClicksService`) are plain Kotlin. A Kafka batch listener
adapter maps `ClickEvent` records to `Click` and calls the use case; a ClickHouse adapter implements
the outbound port with one `JdbcTemplate.batchUpdate` per batch. The listener container's `BATCH`
ack mode commits offsets only when the listener returns normally; a `DefaultErrorHandler` with an
unbounded exponential backoff retries a failed batch forever without committing.

**Tech Stack:** Kotlin 2.4 / JDK 25, Spring Boot 4.1.2, Spring for Apache Kafka 4.1, Spring JDBC,
`com.clickhouse:clickhouse-jdbc` 0.11.0, Jackson 3, JUnit 5, MockK, Testcontainers 2.0.5.

**Spec:** `docs/superpowers/specs/2026-11-08-shared-contracts-design.md` (item B, plus sections 1,
2, 5 and 6 it depends on).

## Global Constraints

- Edit only `services/stream-processor/`. Do not touch `infra/`, `gradle/libs.versions.toml`,
  `build-logic/`, `README.md`, `docs/architecture.md` or other services. Every dependency below is
  already in the catalog.
- Gradle project `:services:stream-processor`, convention `kotlin-spring-service-convention`, base
  package `com.wmbatista.adclick.streamprocessor`.
- Package layout: `domain`, `application.port.inbound`, `application.port.outbound`,
  `application.service`, `adapter.inbound.kafka`, `adapter.outbound.clickhouse`, `config`.
  `domain` and `application` must not import Spring, Kafka, JDBC or `ClickEvent`.
- Topic `click-events`; consumer group `stream-processor`; `auto.offset.reset=earliest`;
  `enable.auto.commit=false`; `max.poll.records=500`; value is `ClickEvent` JSON **without type
  headers**. The service does not declare the topic (no `NewTopic` bean).
- Table `ad_clicks.clicks_per_minute (ad_id String, minute DateTime('UTC'), click_count UInt64)`,
  `SummingMergeTree`. **Insert only** delta rows; never `UPDATE`. Test queries read with
  `sum(click_count) ... GROUP BY`, never raw `click_count`.
- `minute` = `ClickEvent.timestamp` truncated to the minute in UTC.
- Offsets are never committed before the insert succeeded. Insert failure → retry with backoff, no
  commit. Undeserializable record → logged and skipped.
- No deduplication by `clickId` (TD-1, out of scope).
- Config: `KAFKA_BOOTSTRAP_SERVERS` (default `localhost:9092`), `CLICKHOUSE_URL` (default
  `jdbc:clickhouse://localhost:8123/ad_clicks`), `CLICKHOUSE_USER` / `CLICKHOUSE_PASSWORD` (default
  `ad_clicks`). HTTP port `8082` (`SERVER_PORT` override), only `GET /actuator/health` exposed.
- Testcontainers images: `apache/kafka:4.2.2`, `clickhouse/clickhouse-server:26.8.36.3`. Tests load
  the schema from `infra/clickhouse/init/001-schema.sql` (read, never copied).
- Commit messages: imperative sentence, no prefix (repo style), ending with
  `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.

## Review Focus

1. **Batch of only bad records** (all undeserializable, tombstones or blank `adId`) — nothing is
   inserted, no exception, offsets still committed so the consumer does not loop. Tests in Tasks 2
   and 4.
2. **JVM default time zone not UTC** — `minute` written to ClickHouse must still be the UTC minute
   (no 3-hour shift on a developer laptop in `America/Sao_Paulo`). Module tests run with
   `user.timezone=America/Sao_Paulo`; asserted in Task 3.
3. **ClickHouse outage longer than the default retry budget** — `DefaultErrorHandler`'s default
   backoff gives up after 10 attempts and *skips* the batch; ours must never stop. Tests in Task 4
   (backoff unit test + outage end-to-end test).
4. **Minute boundaries** — `10:00:59.999Z` and `10:01:00Z` land in different buckets; the same
   `(adId, minute)` arriving in two batches sums correctly in ClickHouse. Tests in Tasks 2 and 3.
5. **Producer adds a field to `ClickEvent` JSON** — unknown properties must not make every record
   undeserializable (which would silently drop all clicks). Test in Task 1.

---

### Task 1: Spring Boot module and consumer configuration

**Files:**
- Modify: `services/stream-processor/build.gradle.kts`
- Delete: `services/stream-processor/src/main/kotlin/com/wmbatista/adclick/streamprocessor/Main.kt`
- Create: `services/stream-processor/src/main/kotlin/com/wmbatista/adclick/streamprocessor/StreamProcessorApplication.kt`
- Create: `services/stream-processor/src/main/kotlin/com/wmbatista/adclick/streamprocessor/config/StreamProcessorProperties.kt`
- Create: `services/stream-processor/src/main/resources/application.yml`
- Test: `services/stream-processor/src/test/kotlin/com/wmbatista/adclick/streamprocessor/config/ConsumerConfigurationTest.kt`

**Interfaces:**
- Produces: `StreamProcessorApplication` (`@SpringBootApplication @ConfigurationPropertiesScan`),
  main class `com.wmbatista.adclick.streamprocessor.StreamProcessorApplicationKt`.
- Produces: `@ConfigurationProperties("stream-processor") data class StreamProcessorProperties(val topic: String, val retry: Retry)`
  with `data class Retry(val initialInterval: Duration, val multiplier: Double, val maxInterval: Duration)`.
  Defaults in yml: `topic: click-events`, `retry.initial-interval: 500ms`, `retry.multiplier: 2.0`,
  `retry.max-interval: 30s`.

- [ ] **Step 1: Gradle build.** Switch the plugin to `kotlin-spring-service-convention` and set
  `mainClass` as above. Dependencies: `project(":libs:click-event")`,
  `libs.spring.boot.starter.webmvc` (Actuator over HTTP), `libs.spring.boot.starter.kafka`,
  `libs.spring.boot.starter.jdbc`, `libs.spring.boot.starter.actuator`, `libs.jackson.module.kotlin`,
  `runtimeOnly(libs.clickhouse.jdbc)`; test: `libs.spring.boot.testcontainers`,
  `libs.testcontainers.junit.jupiter`, `libs.testcontainers.clickhouse`, `libs.testcontainers.kafka`.
  In `tasks.test`: `systemProperty("user.timezone", "America/Sao_Paulo")` and
  `systemProperty("clickhouse.schema", rootProject.file("infra/clickhouse/init/001-schema.sql").absolutePath)`.
  (If the ClickHouse driver fails at runtime for a missing compression class, add the missing
  artifact as a test/runtime dependency with a comment — do not edit the catalog; report it.)

- [ ] **Step 2: `application.yml`.**

```yaml
spring:
  application.name: stream-processor
  kafka:
    bootstrap-servers: ${KAFKA_BOOTSTRAP_SERVERS:localhost:9092}
    consumer:
      group-id: stream-processor
      auto-offset-reset: earliest
      enable-auto-commit: false
      max-poll-records: 500
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      # A record that cannot be deserialized arrives with a null value instead of failing the poll.
      value-deserializer: org.springframework.kafka.support.serializer.ErrorHandlingDeserializer
      properties:
        spring.deserializer.value.delegate.class: org.springframework.kafka.support.serializer.JacksonJsonDeserializer
        spring.json.value.default.type: com.wmbatista.adclick.event.ClickEvent
        spring.json.use.type.headers: false
    listener:
      type: batch
      # Offsets of a poll are committed only after the listener returned normally.
      ack-mode: batch
  datasource:
    url: ${CLICKHOUSE_URL:jdbc:clickhouse://localhost:8123/ad_clicks}
    username: ${CLICKHOUSE_USER:ad_clicks}
    password: ${CLICKHOUSE_PASSWORD:ad_clicks}
    driver-class-name: com.clickhouse.jdbc.Driver

server:
  port: ${SERVER_PORT:8082}

management:
  endpoints.web.exposure.include: health

stream-processor:
  topic: click-events
  retry:
    initial-interval: 500ms
    multiplier: 2.0
    max-interval: 30s
```

- [ ] **Step 3: Write the failing test.** `ConsumerConfigurationTest` is a
  `@SpringBootTest(webEnvironment = NONE, properties = ["spring.kafka.listener.auto-startup=false"])`
  (needs no broker or ClickHouse) that injects `KafkaProperties` and builds the value deserializer
  from `kafkaProperties.buildConsumerProperties()` (instantiate the configured class, call
  `configure(props, false)`):

```kotlin
@Test fun `consumer settings match the contract`() {
    val props = kafkaProperties.buildConsumerProperties()
    assertEquals("stream-processor", props[ConsumerConfig.GROUP_ID_CONFIG])
    assertEquals("earliest", props[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG])
    assertEquals(false, props[ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG])
    assertEquals(500, props[ConsumerConfig.MAX_POLL_RECORDS_CONFIG])
}

@Test fun `click event json without type headers deserializes`() {
    val json = """{"clickId":"00000000-0000-0000-0000-000000000001","adId":"ad-42","timestamp":"2026-11-06T12:34:56Z"}"""
    assertEquals(
        ClickEvent(UUID.fromString("00000000-0000-0000-0000-000000000001"), "ad-42", Instant.parse("2026-11-06T12:34:56Z")),
        deserializer.deserialize("click-events", RecordHeaders(), json.toByteArray()),
    )
}

@Test fun `unknown json fields are ignored`()          // same JSON plus "campaign":"x" → same ClickEvent
@Test fun `malformed value deserializes to null`()     // bytes "not json" → null, no exception thrown
@Test fun `missing adId deserializes to null`()        // {"clickId":..., "timestamp":...} → null
```

- [ ] **Step 4: Run it to verify it fails.**
  Run: `./gradlew :services:stream-processor:test --tests '*ConsumerConfigurationTest'`
  Expected: compilation failure (no `StreamProcessorApplication`).
- [ ] **Step 5: Implement** `StreamProcessorApplication.kt` (same shape as
  `ClickServiceApplication`) and `StreamProcessorProperties.kt`; delete `Main.kt`.
- [ ] **Step 6: Run the test.** Same command. Expected: 5 tests PASS. Also
  `./gradlew :services:stream-processor:bootJar` succeeds and produces
  `services/stream-processor/build/libs/stream-processor.jar`.
- [ ] **Step 7: Commit** — `Bootstrap stream processor as a Spring Boot Kafka consumer`.

---

### Task 2: Domain aggregation and use case

**Files:**
- Create: `.../streamprocessor/domain/Click.kt`, `.../domain/ClickCount.kt`, `.../domain/ClickAggregation.kt`
- Create: `.../application/port/inbound/AggregateClicksUseCase.kt`
- Create: `.../application/port/outbound/SaveClickCountsPort.kt`
- Create: `.../application/service/AggregateClicksService.kt`
- Test: `.../domain/ClickAggregationTest.kt`, `.../application/service/AggregateClicksServiceTest.kt`

(`...` = `services/stream-processor/src/{main,test}/kotlin/com/wmbatista/adclick/streamprocessor`)

**Interfaces:**
- Produces: `data class Click(val adId: String, val timestamp: Instant)`
- Produces: `data class ClickCount(val adId: String, val minute: Instant, val count: Long)`
  (`minute` is always a whole UTC minute).
- Produces: `fun aggregatePerMinute(clicks: List<Click>): List<ClickCount>` — top-level in
  `ClickAggregation.kt`; one entry per distinct `(adId, timestamp.truncatedTo(ChronoUnit.MINUTES))`,
  sorted by `adId` then `minute`.
- Produces: `fun interface AggregateClicksUseCase { fun aggregate(clicks: List<Click>) }`
- Produces: `fun interface SaveClickCountsPort { fun save(counts: List<ClickCount>) }` — throws on
  failure; the caller must not commit.
- Produces: `class AggregateClicksService(private val saveClickCountsPort: SaveClickCountsPort) : AggregateClicksUseCase`
  — aggregates and calls `save` exactly once if there is at least one count, never with an empty
  list; exceptions from the port propagate unchanged.

- [ ] **Step 1: Write the failing tests.**

```kotlin
// ClickAggregationTest
@Test fun `clicks in the same minute and ad are counted together`()
    // ad-1 at 10:00:00Z, 10:00:30Z, 10:00:59.999Z → [ClickCount("ad-1", 10:00:00Z, 3)]
@Test fun `minute boundary starts a new bucket`()
    // ad-1 at 10:00:59.999Z and 10:01:00Z → [(ad-1, 10:00, 1), (ad-1, 10:01, 1)]
@Test fun `different ads are counted separately`()
    // ad-2 10:00:10Z, ad-1 10:00:20Z, ad-2 10:00:40Z → [(ad-1, 10:00, 1), (ad-2, 10:00, 2)]
@Test fun `no clicks gives no counts`()   // emptyList() → emptyList()

// AggregateClicksServiceTest (MockK)
@Test fun `saves the per-minute counts once`()
    // two clicks ad-1 in 10:00 → verify(exactly = 1) { port.save(listOf(ClickCount("ad-1", 10:00Z, 2))) }
@Test fun `empty batch does not touch the port`()  // aggregate(emptyList()) → verify { port wasNot Called }
@Test fun `port failure propagates`()
    // every { port.save(any()) } throws IllegalStateException("down") → assertThrows<IllegalStateException>
```

- [ ] **Step 2: Run them to verify they fail.**
  Run: `./gradlew :services:stream-processor:test --tests '*ClickAggregationTest' --tests '*AggregateClicksServiceTest'`
  Expected: compilation failure.
- [ ] **Step 3: Implement** the types and functions listed under Interfaces.
- [ ] **Step 4: Run the tests.** Same command. Expected: 7 tests PASS.
- [ ] **Step 5: Commit** — `Aggregate clicks per ad and minute`.

---

### Task 3: ClickHouse writer

**Files:**
- Create: `.../adapter/outbound/clickhouse/ClickHouseClickCountWriter.kt`
- Test: `.../adapter/outbound/clickhouse/ClickHouseClickCountWriterTest.kt`
- Test: `.../support/ClickHouseTestContainer.kt` (shared with Task 4)

**Interfaces:**
- Consumes: `SaveClickCountsPort`, `ClickCount` (Task 2).
- Produces: `class ClickHouseClickCountWriter(private val jdbcTemplate: JdbcTemplate) : SaveClickCountsPort`
  — one `jdbcTemplate.batchUpdate("INSERT INTO clicks_per_minute (ad_id, minute, click_count) VALUES (?, ?, ?)", ...)`
  per call (the driver sends a JDBC batch as a single insert). Bind `minute` as
  `LocalDateTime.ofInstant(minute, ZoneOffset.UTC)` so the JVM time zone never shifts it; the column
  is `DateTime('UTC')`. Empty list → no statement. Let `DataAccessException` propagate.
- Produces (test support): `object ClickHouseTestContainer` exposing a started, lazily shared
  `org.testcontainers.clickhouse.ClickHouseContainer("clickhouse/clickhouse-server:26.8.36.3")`
  with user/password `ad_clicks`, database `ad_clicks`, and the file from system property
  `clickhouse.schema` copied to `/docker-entrypoint-initdb.d/001-schema.sql` (same mechanism as
  Compose); plus `fun jdbcUrl(): String`, `fun dataSource(): DataSource`, and
  `fun clickCounts(jdbc: JdbcTemplate): Map<Pair<String, Instant>, Long>` that runs
  `SELECT ad_id, toUnixTimestamp(minute) AS m, sum(click_count) AS c FROM clicks_per_minute GROUP BY ad_id, m`
  and maps `m` with `Instant.ofEpochSecond`. Truncate the table in each test's `@BeforeEach`.

- [ ] **Step 1: Write the failing test** (plain JUnit + Testcontainers, no Spring context):

```kotlin
@Test fun `inserted counts can be read back by ad and utc minute`() {
    writer.save(listOf(ClickCount("ad-1", Instant.parse("2026-11-08T10:00:00Z"), 3),
                       ClickCount("ad-2", Instant.parse("2026-11-08T10:01:00Z"), 1)))
    assertEquals(mapOf(("ad-1" to Instant.parse("2026-11-08T10:00:00Z")) to 3L,
                       ("ad-2" to Instant.parse("2026-11-08T10:01:00Z")) to 1L), clickCounts(jdbc))
}
@Test fun `deltas for the same ad and minute add up`()
    // save [(ad-1, 10:00, 3)] then [(ad-1, 10:00, 4)] → {(ad-1, 10:00): 7}
@Test fun `jvm time zone is not utc in tests`()
    // assertNotEquals(ZoneOffset.UTC, ZoneId.systemDefault().rules.getOffset(Instant.now())) — guards Review Focus 2
@Test fun `empty list writes nothing`()   // save(emptyList()) → clickCounts(jdbc) is empty, no exception
@Test fun `missing table fails the save`()
    // jdbc.execute("RENAME TABLE clicks_per_minute TO clicks_per_minute_off"); try { assertThrows<DataAccessException> { save(one count) } } finally { rename back }
```

- [ ] **Step 2: Run it to verify it fails.**
  Run: `./gradlew :services:stream-processor:test --tests '*ClickHouseClickCountWriterTest'`
  Expected: compilation failure.
- [ ] **Step 3: Implement** `ClickHouseClickCountWriter` and `ClickHouseTestContainer` as above.
- [ ] **Step 4: Run the test.** Same command. Expected: 5 tests PASS (Docker required).
- [ ] **Step 5: Commit** — `Write per-minute click counts to ClickHouse`.

---

### Task 4: Kafka listener, retry policy and end-to-end test

**Files:**
- Create: `.../adapter/inbound/kafka/ClickEventsListener.kt`
- Create: `.../config/StreamProcessorConfiguration.kt`
- Test: `.../adapter/inbound/kafka/ClickEventsListenerTest.kt`
- Test: `.../config/StreamProcessorConfigurationTest.kt`
- Test: `.../StreamProcessorIntegrationTest.kt`

**Interfaces:**
- Consumes: `AggregateClicksUseCase`, `Click`, `AggregateClicksService`, `SaveClickCountsPort`
  (Task 2); `ClickHouseClickCountWriter`, `ClickHouseTestContainer` (Task 3);
  `StreamProcessorProperties` (Task 1).
- Produces: `class ClickEventsListener(private val aggregateClicks: AggregateClicksUseCase)` with
  `@KafkaListener(topics = ["\${stream-processor.topic}"]) fun onBatch(records: List<ConsumerRecord<String, ClickEvent?>>)`.
  Maps records with a non-null value and non-blank `adId` to `Click(adId, timestamp)` and calls
  `aggregateClicks.aggregate(...)` once per batch (also with an empty list — the service ignores
  it). Skipped records are logged at WARN with `topic-partition@offset` and the reason (for a null
  value, include the exception from
  `SerializationUtils.getExceptionFromHeader(record, SerializationUtils.VALUE_DESERIALIZER_EXCEPTION_HEADER, ...)`
  if present). Exceptions from the use case propagate (the container must not commit).
- Produces: `StreamProcessorConfiguration` beans: `saveClickCountsPort(jdbcTemplate)` →
  `ClickHouseClickCountWriter`; `aggregateClicksUseCase(port)` → `AggregateClicksService`;
  `clickEventsListener(useCase)`; `clickBatchBackOff(properties): BackOff` → Spring
  `ExponentialBackOff(initialInterval, multiplier)` with `maxInterval` from properties and **no
  attempt or elapsed-time limit**; `kafkaErrorHandler(backOff): CommonErrorHandler` →
  `DefaultErrorHandler(backOff)` (Boot wires a `CommonErrorHandler` bean into the listener container
  factory; a non-`BatchListenerFailedException` from a batch listener retries the whole batch).

- [ ] **Step 1: Write the failing unit tests.**

```kotlin
// ClickEventsListenerTest (MockK use case, ConsumerRecord built by hand)
@Test fun `maps every valid record to a click in one call`()
    // two records ad-1@10:00:05Z, ad-2@10:00:07Z → verify(exactly = 1) { useCase.aggregate(listOf(Click("ad-1", ...), Click("ad-2", ...))) }
@Test fun `null values and blank ad ids are skipped`()
    // [valid ad-1, value null, ClickEvent with adId "  "] → aggregate(listOf(Click("ad-1", ...)))
@Test fun `batch with only bad records aggregates nothing and does not throw`()
    // [null, null] → aggregate(emptyList())
@Test fun `use case failure propagates`()
    // use case throws RuntimeException → assertThrows<RuntimeException> { listener.onBatch(...) }

// StreamProcessorConfigurationTest (no Spring context; build properties by hand)
@Test fun `batch backoff never gives up`()
    // val execution = configuration.clickBatchBackOff(props).start(); repeat(10_000) { assertNotEquals(BackOffExecution.STOP, execution.nextBackOff()) }
@Test fun `batch backoff is capped at max interval`()
    // retry(500ms, 2.0, 30s): after 20 calls nextBackOff() == 30_000
```

- [ ] **Step 2: Run them to verify they fail.**
  Run: `./gradlew :services:stream-processor:test --tests '*ClickEventsListenerTest' --tests '*StreamProcessorConfigurationTest'`
  Expected: compilation failure.
- [ ] **Step 3: Implement** `ClickEventsListener` and `StreamProcessorConfiguration` as above.
- [ ] **Step 4: Run them.** Same command. Expected: 6 tests PASS.

- [ ] **Step 5: Write the end-to-end test.** `StreamProcessorIntegrationTest` is a
  `@SpringBootTest(webEnvironment = NONE)` with a static
  `@Container @ServiceConnection KafkaContainer("apache/kafka:4.2.2")` (`org.testcontainers.kafka`),
  ClickHouse from `ClickHouseTestContainer` wired with `@DynamicPropertySource` (`spring.datasource.*`),
  and test properties `stream-processor.retry.initial-interval=100ms`,
  `stream-processor.retry.max-interval=500ms`. In `@BeforeAll`, create topic `click-events` with
  3 partitions via `AdminClient` (the service must not create it). Produce with a raw
  `KafkaProducer<String, String>` (key `adId`, JSON string value, no headers) so the test controls
  the exact bytes. Helpers: `committedOffsets(): Map<TopicPartition, Long>` via
  `admin.listConsumerGroupOffsets("stream-processor")` and `endOffsets()` via `admin.listOffsets`.
  Await with a polling loop (timeout 30 s) — no extra dependency. Truncate the table between tests.

```kotlin
@Test fun `clicks are counted per ad and minute and offsets committed`() {
    // send ad-1 @10:00:05Z, ad-1 @10:00:50Z, ad-1 @10:01:00Z, ad-2 @10:00:30Z, plus the value "not json" keyed ad-1
    // await clickCounts(jdbc) == {(ad-1,10:00):2, (ad-1,10:01):1, (ad-2,10:00):1}
    // await committedOffsets() == endOffsets()   // the bad record was skipped, not blocking
}

@Test fun `clickhouse outage holds offsets until the insert succeeds`() {
    // RENAME TABLE clicks_per_minute TO clicks_per_minute_off
    // record committed = committedOffsets(); send 2 clicks for ad-3 @10:00:10Z
    // wait 3 s (≥ 6 retries at 500 ms): committedOffsets() == committed (no commit while failing)
    // RENAME TABLE clicks_per_minute_off TO clicks_per_minute
    // await clickCounts(jdbc) == {(ad-3,10:00):2} and committedOffsets() == endOffsets()
}
```

- [ ] **Step 6: Run it.**
  Run: `./gradlew :services:stream-processor:test --tests '*StreamProcessorIntegrationTest'`
  Expected: 2 tests PASS. If the first fails, fix the implementation, not the expectations.
- [ ] **Step 7: Run the whole build.**
  Run: `./gradlew build`
  Expected: BUILD SUCCESSFUL, all modules' tests pass.
- [ ] **Step 8: Manual smoke check** (optional, needs the Compose stack from A): with
  `docker compose -f infra/compose.yaml up -d kafka kafka-init clickhouse` running, start
  `./gradlew :services:stream-processor:bootRun`, `curl -fsS localhost:8082/actuator/health` returns
  `{"status":"UP"...}`. Adding the service to `infra/compose.yaml` is owned by A / the integration
  step — do not edit it.
- [ ] **Step 9: Commit** — `Consume click events in batches and commit only after ClickHouse insert`.
