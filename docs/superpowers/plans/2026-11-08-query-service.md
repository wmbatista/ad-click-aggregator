# Query Service Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `services/query-service` serves `GET /ads/{adId}/clicks?from&to&granularity` from ClickHouse, returning zero-filled click buckets per minute, hour or day.

**Architecture:** Spring Boot service in the same hexagonal layout as `click-service`: a pure-Kotlin domain (`ClickQuery` validates and normalizes the range, `ClickMetrics` is the result), an application service that zero-fills the buckets, a ClickHouse outbound adapter on `JdbcTemplate` that always reads `sum(click_count)`, and a web inbound adapter that maps errors to `400`/`503`.

**Tech Stack:** Kotlin 2.4, Spring Boot 4.1.2 (webmvc, jdbc, actuator), `com.clickhouse:clickhouse-jdbc:0.11.0`, JUnit 5, MockK, Testcontainers 2.0.5 (`testcontainers-clickhouse`).

**Spec:** `docs/superpowers/specs/2026-11-08-shared-contracts-design.md` (work item **C**, sections 1, 4, 5 and 6).

## Global Constraints

- Package `com.wmbatista.adclick.queryservice`; module uses `kotlin-spring-service-convention`.
- Endpoint `GET /ads/{adId}/clicks?from=<instant>&to=<instant>&granularity=<MINUTE|HOUR|DAY>`; `from` inclusive, `to` exclusive, both required, `from < to`; `granularity` defaults to `MINUTE`.
- `from` truncated down and `to` rounded up to the granularity; at most **1,440 buckets**, more is `400`.
- Buckets zero-filled and in order; unknown `adId` returns all zeros. Invalid/missing parameters `400`; ClickHouse unavailable `503`.
- Reads only with `sum(click_count)` + `GROUP BY` on `toStartOfMinute/Hour/Day(minute)`; never read `click_count` directly.
- Table `ad_clicks.clicks_per_minute (ad_id String, minute DateTime('UTC'), click_count UInt64)`; tests load the schema from `infra/clickhouse/init/001-schema.sql` (do not copy or edit it).
- Env vars: `CLICKHOUSE_URL` (default `jdbc:clickhouse://localhost:8123/ad_clicks`), `CLICKHOUSE_USER` (default `ad_clicks`), `CLICKHOUSE_PASSWORD` (default `ad_clicks`). Port `8081`. Only `GET /actuator/health` exposed from Actuator.
- Testcontainers image `clickhouse/clickhouse-server:26.8.36.3`.
- File ownership: edit only `services/query-service/`. Do not touch `gradle/libs.versions.toml` (entries already exist), `infra/`, other services, `README.md` or `docs/architecture.md`. No deduplication logic (TD-1).

**Decisions made while planning (not in the spec):**

- Instants are passed to and read from ClickHouse as Unix epoch seconds (`toDateTime(?, 'UTC')`, `toUnixTimestamp(...)`), so no driver or JVM time zone can shift a bucket.
- The normalized range must lie within `[1970-01-01T00:00:00Z, 2106-01-01T00:00:00Z]` (ClickHouse `DateTime` range); outside it is `400`.
- `granularity` is case-sensitive (`MINUTE`, `HOUR`, `DAY`); anything else is `400`.
- `400` responses for domain validation carry a `ProblemDetail` with the reason; Spring's own binding errors (missing/unparseable parameter) keep Spring's default `400`.
- Hikari `connection-timeout: 2s`, so an unreachable ClickHouse answers `503` in seconds rather than after 30 s.
- The Compose entry for query-service is **not** part of this plan; it is added in the integration step (as the infrastructure plan did for click-service's siblings).

## Review Focus

1. **Unmerged delta rows** — several rows for the same `(ad_id, minute)` exist until ClickHouse merges; expected: their sum, never one of them. Pinned in Task 3 (`sums delta rows that have not been merged`).
2. **Unaligned `from`/`to` and the exclusive end** — e.g. `from=10:00:30Z`, `to=10:02:00Z`; expected: buckets 10:00 and 10:01 only, a row at exactly `to` excluded. Pinned in Task 1 (normalization tests) and Task 3 (`includes from and excludes to`).
3. **Instants with an offset or fractional seconds** — `2026-11-08T07:00:00-03:00` or `...10:00:00.500Z`; expected: treated as the same UTC instant, not `400` or a shifted bucket. Pinned in Task 4 (`accepts an instant with an offset`) and Task 1 (fractional truncation).
4. **Ranges ClickHouse cannot represent** — before 1970 or after 2105; expected: `400` with a reason, not a `500` or silently wrong data. Pinned in Task 1 (`rejects ranges outside the ClickHouse DateTime range`).
5. **ClickHouse down** — expected: `503` within a few seconds, not `500` and not a 30 s hang. Pinned in Task 3 (`unreachable ClickHouse throws ClickStoreUnavailableException`) and Task 5 (`ClickHouseDownTest`).

---

### Task 1: Module setup and query domain

**Files:**
- Modify: `services/query-service/build.gradle.kts`
- Delete: `services/query-service/src/main/kotlin/com/wmbatista/adclick/queryservice/Main.kt`
- Create: `services/query-service/src/main/kotlin/com/wmbatista/adclick/queryservice/QueryServiceApplication.kt`
- Create: `.../queryservice/domain/Granularity.kt`, `.../queryservice/domain/ClickQuery.kt`, `.../queryservice/domain/InvalidClickQueryException.kt`, `.../queryservice/domain/ClickMetrics.kt`
- Test: `services/query-service/src/test/kotlin/com/wmbatista/adclick/queryservice/domain/ClickQueryTest.kt`

(`...` = `services/query-service/src/main/kotlin/com/wmbatista/adclick`.)

**Interfaces:**
- Produces:
  - `enum class Granularity(val unit: ChronoUnit) { MINUTE(MINUTES), HOUR(HOURS), DAY(DAYS) }`
  - `class ClickQuery private constructor(val adId: String, val from: Instant, val to: Instant, val granularity: Granularity)` with `companion fun of(adId: String, from: Instant, to: Instant, granularity: Granularity): ClickQuery` (throws `InvalidClickQueryException`) and `val bucketStarts: List<Instant>`. `from`/`to` are the **normalized** values.
  - `class InvalidClickQueryException(message: String) : RuntimeException(message)`
  - `data class ClickBucket(val start: Instant, val count: Long)`
  - `data class ClickMetrics(val adId: String, val granularity: Granularity, val from: Instant, val to: Instant, val buckets: List<ClickBucket>)` with `val total: Long` = sum of bucket counts.
  - `ClickQuery.MAX_BUCKETS = 1440`.

- [ ] **Step 1: Switch the module to Spring Boot.** `build.gradle.kts`: plugin `kotlin-spring-service-convention`; `implementation` of `libs.spring.boot.starter.webmvc`, `.jdbc`, `.actuator`, `libs.jackson.module.kotlin`; `runtimeOnly(libs.clickhouse.jdbc)`; `testImplementation` of `libs.spring.boot.testcontainers`, `libs.testcontainers.junit.jupiter`, `libs.testcontainers.clickhouse`; drop the `libs:click-event` dependency (unused here); `mainClass = "com.wmbatista.adclick.queryservice.QueryServiceApplicationKt"`; in `tasks.test`, `systemProperty("clickhouse.schema", rootProject.file("infra/clickhouse/init/001-schema.sql").absolutePath)`. Replace `Main.kt` with `QueryServiceApplication` shaped like `ClickServiceApplication` (no `@ConfigurationPropertiesScan` needed).

- [ ] **Step 2: Write the failing tests in `ClickQueryTest`**

```kotlin
private fun t(s: String) = Instant.parse(s)

@Test fun `truncates from down and rounds to up to the granularity`() {
    val q = ClickQuery.of("ad-1", t("2026-11-08T10:00:30.500Z"), t("2026-11-08T10:02:00.001Z"), MINUTE)
    assertEquals(t("2026-11-08T10:00:00Z"), q.from)
    assertEquals(t("2026-11-08T10:03:00Z"), q.to)
    assertEquals(listOf(t("2026-11-08T10:00:00Z"), t("2026-11-08T10:01:00Z"), t("2026-11-08T10:02:00Z")), q.bucketStarts)
}
@Test fun `an aligned to is kept`()                // to=10:02:00Z, MINUTE -> q.to == 10:02:00Z, 2 buckets
@Test fun `hour and day buckets are UTC-aligned`()  // from=2026-11-08T10:15Z to=2026-11-08T12:00:01Z HOUR -> [10:00,11:00,12:00]; DAY from 2026-11-08T23:00Z to 2026-11-09T01:00Z -> [2026-11-08T00:00Z, 2026-11-09T00:00Z]
@Test fun `allows exactly 1440 buckets`()           // from=2026-11-08T00:00Z to=2026-11-09T00:00Z MINUTE -> bucketStarts.size == 1440
@Test fun `rejects 1441 buckets`()                  // to=2026-11-09T00:01Z -> assertThrows<InvalidClickQueryException>
@ParameterizedTest fun `rejects from not before to`()  // from == to, and from > to (checked on the raw input, before normalization)
@Test fun `rejects a blank adId`()                  // " "
@ParameterizedTest fun `rejects ranges outside the ClickHouse DateTime range`()
    // [1969-12-31T23:59:00Z, 1970-01-01T00:01:00Z) and [2105-12-31T23:59:00Z, 2106-01-01T00:01:00Z)
```

- [ ] **Step 3: Run to verify they fail**

Run: `./gradlew :services:query-service:test --tests '*ClickQueryTest'`
Expected: compilation failure (`ClickQuery` unresolved).

- [ ] **Step 4: Implement the domain types.** `ClickQuery.of` checks, in order: `adId` not blank; raw `from < to`; normalize (`from.truncatedTo(unit)`; `to` truncated, plus one `unit` if it was not aligned); normalized range within `[1970-01-01T00:00:00Z, 2106-01-01T00:00:00Z]`; bucket count `Duration.between(from, to).dividedBy(unit.duration) <= MAX_BUCKETS`. Each failure message names the parameter and the limit (it is shown to the caller).

- [ ] **Step 5: Run to verify they pass**

Run: `./gradlew :services:query-service:test --tests '*ClickQueryTest'`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add services/query-service
git commit -m "Add query-service domain: click query normalization and validation"
```

---

### Task 2: Get-click-metrics use case

**Files:**
- Create: `.../queryservice/application/port/inbound/GetClickMetricsUseCase.kt`
- Create: `.../queryservice/application/port/outbound/LoadClickCountsPort.kt`, `.../application/port/outbound/ClickStoreUnavailableException.kt`
- Create: `.../queryservice/application/service/GetClickMetricsService.kt`
- Test: `services/query-service/src/test/kotlin/com/wmbatista/adclick/queryservice/application/service/GetClickMetricsServiceTest.kt`

**Interfaces:**
- Consumes: `ClickQuery`, `ClickMetrics`, `ClickBucket` (Task 1).
- Produces:
  - `interface GetClickMetricsUseCase { fun getClickMetrics(query: ClickQuery): ClickMetrics }` (KDoc: throws `ClickStoreUnavailableException`).
  - `interface LoadClickCountsPort { fun countsPerBucket(query: ClickQuery): Map<Instant, Long> }` — keys are bucket starts; buckets without clicks are absent.
  - `class ClickStoreUnavailableException(message: String, cause: Throwable? = null) : RuntimeException(message, cause)`
  - `class GetClickMetricsService(private val loadClickCounts: LoadClickCountsPort) : GetClickMetricsUseCase`

- [ ] **Step 1: Write the failing tests** (MockK for the port; query `ad-123`, `10:00Z`–`10:03Z`, `MINUTE`)

```kotlin
@Test fun `zero-fills missing buckets in order`() {
    every { port.countsPerBucket(query) } returns mapOf(t("2026-11-08T10:00:00Z") to 5L, t("2026-11-08T10:02:00Z") to 2L)
    val m = service.getClickMetrics(query)
    assertEquals(listOf(ClickBucket(t("…10:00:00Z"), 5), ClickBucket(t("…10:01:00Z"), 0), ClickBucket(t("…10:02:00Z"), 2)), m.buckets)
    assertEquals(7, m.total)
    assertEquals(query.from, m.from); assertEquals(query.to, m.to); assertEquals(MINUTE, m.granularity); assertEquals("ad-123", m.adId)
}
@Test fun `no rows gives all zeros`()                     // port returns emptyMap -> 3 buckets of 0, total 0
@Test fun `store failure propagates`()                    // port throws ClickStoreUnavailableException -> assertThrows same
```

- [ ] **Step 2: Run to verify they fail**

Run: `./gradlew :services:query-service:test --tests '*GetClickMetricsServiceTest'`
Expected: compilation failure.

- [ ] **Step 3: Implement the ports and `GetClickMetricsService`** (map `query.bucketStarts` to `ClickBucket(start, counts[start] ?: 0)`; keys not in `bucketStarts` are ignored).

- [ ] **Step 4: Run to verify they pass** — same command, PASS.

- [ ] **Step 5: Commit**

```bash
git add services/query-service
git commit -m "Add get-click-metrics use case with zero-filled buckets"
```

---

### Task 3: ClickHouse adapter

**Files:**
- Create: `.../queryservice/adapter/outbound/clickhouse/ClickHouseClickCountsAdapter.kt`
- Create (test support): `services/query-service/src/test/kotlin/com/wmbatista/adclick/queryservice/ClickHouseTestContainer.kt`
- Test: `services/query-service/src/test/kotlin/com/wmbatista/adclick/queryservice/adapter/outbound/clickhouse/ClickHouseClickCountsAdapterTest.kt`

**Interfaces:**
- Consumes: `LoadClickCountsPort`, `ClickStoreUnavailableException` (Task 2); `ClickQuery`, `Granularity` (Task 1).
- Produces:
  - `class ClickHouseClickCountsAdapter(private val jdbcTemplate: JdbcTemplate) : LoadClickCountsPort`
  - Test support `object ClickHouseTestContainer { val container: ClickHouseContainer; fun jdbcTemplate(): JdbcTemplate; fun insert(adId: String, minute: Instant, count: Long); fun truncate() }` — one container per JVM, started lazily: `ClickHouseContainer("clickhouse/clickhouse-server:26.8.36.3")` with database/user/password `ad_clicks`, and the file from system property `clickhouse.schema` copied to `/docker-entrypoint-initdb.d/001-schema.sql`. `insert` runs **one INSERT per call** (so separate calls create separate, unmerged parts). Task 5 reuses it.

- [ ] **Step 1: Write the failing tests** (truncate the table before each test)

```kotlin
@Test fun `sums delta rows that have not been merged`() {
    insert("ad-1", t("2026-11-08T10:00:00Z"), 2); insert("ad-1", t("2026-11-08T10:00:00Z"), 3)
    assertEquals(mapOf(t("2026-11-08T10:00:00Z") to 5L), adapter.countsPerBucket(ClickQuery.of("ad-1", t("…10:00Z"), t("…10:01Z"), MINUTE)))
}
@Test fun `includes from and excludes to`()          // rows at 10:00 (1), 10:02 (4), 10:03 (8); query 10:00–10:03 MINUTE -> {10:00=1, 10:02=4}
@Test fun `ignores other ads`()                      // ad-2 row in range -> absent from ad-1 result
@Test fun `groups minutes into hours and days`()      // rows 10:05 (1), 10:59 (2), 11:00 (4) -> HOUR {10:00=3, 11:00=4}; DAY {2026-11-08T00:00Z=7}
@Test fun `unreachable ClickHouse throws ClickStoreUnavailableException`()
    // adapter over JdbcTemplate(DriverManagerDataSource("jdbc:clickhouse://localhost:1/ad_clicks", "ad_clicks", "ad_clicks"))
```

- [ ] **Step 2: Run to verify they fail**

Run: `./gradlew :services:query-service:test --tests '*ClickHouseClickCountsAdapterTest'`
Expected: compilation failure. (Docker must be running.)

- [ ] **Step 3: Implement the adapter.** One query, bucket function chosen by `when (granularity)` → `toStartOfMinute` / `toStartOfHour` / `toStartOfDay`:

```sql
SELECT toUnixTimestamp(<bucketFn>(minute)) AS bucket, sum(click_count) AS clicks
FROM ad_clicks.clicks_per_minute
WHERE ad_id = ? AND minute >= toDateTime(?, 'UTC') AND minute < toDateTime(?, 'UTC')
GROUP BY bucket
```

Parameters: `adId`, `from.epochSecond`, `to.epochSecond`; map rows to `Instant.ofEpochSecond(getLong("bucket")) to getLong("clicks")`. Wrap Spring's `DataAccessException` in `ClickStoreUnavailableException`.

- [ ] **Step 4: Run to verify they pass** — same command, PASS.

- [ ] **Step 5: Commit**

```bash
git add services/query-service
git commit -m "Add ClickHouse adapter reading summed click counts"
```

---

### Task 4: HTTP endpoint

**Files:**
- Create: `.../queryservice/adapter/inbound/web/ClickMetricsController.kt`, `.../adapter/inbound/web/ClickMetricsResponse.kt`, `.../adapter/inbound/web/ClickMetricsExceptionHandler.kt`
- Test: `services/query-service/src/test/kotlin/com/wmbatista/adclick/queryservice/adapter/inbound/web/ClickMetricsControllerTest.kt`

**Interfaces:**
- Consumes: `GetClickMetricsUseCase`, `ClickStoreUnavailableException` (Task 2); `ClickQuery.of`, `InvalidClickQueryException`, `Granularity` (Task 1).
- Produces:
  - `@GetMapping("/ads/{adId}/clicks") fun clicks(@PathVariable adId: String, @RequestParam from: Instant, @RequestParam to: Instant, @RequestParam(defaultValue = "MINUTE") granularity: Granularity): ClickMetricsResponse`
  - `data class ClickMetricsResponse(adId: String, granularity: Granularity, from: Instant, to: Instant, total: Long, buckets: List<Bucket>)` with nested `data class Bucket(start: Instant, count: Long)` and `companion fun from(metrics: ClickMetrics)`.
  - Exception handler: `InvalidClickQueryException` → `400` `ProblemDetail` (detail = exception message); `ClickStoreUnavailableException` → `503`, logged at `warn`.

- [ ] **Step 1: Write the failing tests** (standalone MockMvc + MockK use case, same style as `ClickControllerTest`)

```kotlin
@Test fun `returns the spec example`() {
    // use case returns the metrics from spec §4 (ad-123, MINUTE, 10:00–10:03, buckets 5/0/2)
    mockMvc.get("/ads/ad-123/clicks?from=2026-11-08T10:00:00Z&to=2026-11-08T10:03:00Z&granularity=MINUTE").andExpect {
        status { isOk() }
        content { json("""{"adId":"ad-123","granularity":"MINUTE","from":"2026-11-08T10:00:00Z","to":"2026-11-08T10:03:00Z","total":7,
          "buckets":[{"start":"2026-11-08T10:00:00Z","count":5},{"start":"2026-11-08T10:01:00Z","count":0},{"start":"2026-11-08T10:02:00Z","count":2}]}""", JsonCompareMode.STRICT) }
    }
}
@Test fun `granularity defaults to MINUTE`()            // capture the ClickQuery passed to the use case: granularity == MINUTE
@Test fun `accepts an instant with an offset`()          // from=2026-11-08T07:00:00-03:00 (URL-encoded) -> captured query.from == 2026-11-08T10:00:00Z
@ParameterizedTest fun `invalid parameters return 400 without calling the use case`(query: String)
    // "to=…10:03:00Z" (no from), "from=…10:00:00Z" (no to), "from=yesterday&to=…", "from=…&to=…&granularity=WEEK",
    // "from=…&to=…&granularity=minute", "from=…10:03:00Z&to=…10:00:00Z", "from=2026-11-08T00:00:00Z&to=2026-11-09T00:01:00Z"
@Test fun `validation errors explain the reason`()      // from > to -> 400 and jsonPath("$.detail") exists
@Test fun `store unavailable returns 503`()             // use case throws ClickStoreUnavailableException
```

- [ ] **Step 2: Run to verify they fail**

Run: `./gradlew :services:query-service:test --tests '*ClickMetricsControllerTest'`
Expected: compilation failure.

- [ ] **Step 3: Implement controller, response and exception handler** per the Interfaces block. The controller only builds `ClickQuery.of(...)`, calls the use case and maps with `ClickMetricsResponse.from`.

- [ ] **Step 4: Run to verify they pass** — same command, PASS.

- [ ] **Step 5: Commit**

```bash
git add services/query-service
git commit -m "Add GET /ads/{adId}/clicks endpoint"
```

---

### Task 5: Wiring, configuration and end-to-end tests

**Files:**
- Create: `.../queryservice/config/QueryServiceConfiguration.kt`
- Create: `services/query-service/src/main/resources/application.yml`
- Test: `services/query-service/src/test/kotlin/com/wmbatista/adclick/queryservice/QueryServiceApplicationTest.kt`, `services/query-service/src/test/kotlin/com/wmbatista/adclick/queryservice/ClickHouseDownTest.kt`

**Interfaces:**
- Consumes: everything above; `ClickHouseTestContainer` (Task 3).
- Produces: `@Bean`s `loadClickCountsPort(jdbcTemplate: JdbcTemplate): LoadClickCountsPort` and `getClickMetricsUseCase(port: LoadClickCountsPort): GetClickMetricsUseCase`.

`application.yml` values:

```yaml
spring:
  application.name: query-service
  threads.virtual.enabled: true
  datasource:
    url: ${CLICKHOUSE_URL:jdbc:clickhouse://localhost:8123/ad_clicks}
    username: ${CLICKHOUSE_USER:ad_clicks}
    password: ${CLICKHOUSE_PASSWORD:ad_clicks}
    hikari.connection-timeout: 2s
server.port: ${SERVER_PORT:8081}
management.endpoints.web.exposure.include: health
```

- [ ] **Step 1: Write the failing tests.** Both are `@SpringBootTest(webEnvironment = RANDOM_PORT)`; read the port from `Environment.getProperty("local.server.port")` and call it with `java.net.http.HttpClient`.
  - `QueryServiceApplicationTest` (`@DynamicPropertySource` sets `spring.datasource.url/username/password` from `ClickHouseTestContainer.container`):
    - `serves zero-filled buckets from ClickHouse` — insert `ad-123` 10:00 → 2, 10:00 → 3, 10:02 → 2; GET `from=10:00Z&to=10:03Z` → `200`, body has `"total":7` and counts `5, 0, 2`.
    - `health is UP` — GET `/actuator/health` → `200`, body contains `"status":"UP"`.
  - `ClickHouseDownTest` (`spring.datasource.url=jdbc:clickhouse://localhost:1/ad_clicks` via `properties`):
    - `answers 503 within 5 seconds` — GET the clicks endpoint with a valid range → status `503`, elapsed `< 5s`.

- [ ] **Step 2: Run to verify they fail**

Run: `./gradlew :services:query-service:test --tests '*QueryServiceApplicationTest' --tests '*ClickHouseDownTest'`
Expected: FAIL (no `GetClickMetricsUseCase` bean / `404`).

- [ ] **Step 3: Implement `QueryServiceConfiguration` and `application.yml`.**

- [ ] **Step 4: Run the whole build**

Run: `./gradlew build`
Expected: `BUILD SUCCESSFUL`; all query-service tests and the existing click-service tests pass.

- [ ] **Step 5: Smoke test against the Compose ClickHouse**

Run: `docker compose -f infra/compose.yaml up -d --wait clickhouse && ./gradlew :services:query-service:bootRun` and, in another shell, `curl -s 'localhost:8081/ads/ad-1/clicks?from=2026-11-08T10:00:00Z&to=2026-11-08T10:03:00Z'`
Expected: `200` with three zero buckets and `"total":0`.

- [ ] **Step 6: Commit**

```bash
git add services/query-service
git commit -m "Wire query service to ClickHouse and expose health"
```
