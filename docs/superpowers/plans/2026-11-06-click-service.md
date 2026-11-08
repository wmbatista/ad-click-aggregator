# Click Service Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn the `click-processor` placeholder into `click-service`, a Spring Boot service with
hexagonal architecture exposing `POST /click`, which publishes a `ClickEvent` to Kafka and answers
202 only after Kafka acknowledges the write.

**Architecture:** Domain (`Click`) and application layer (inbound port `RegisterClickUseCase`,
outbound ports `PublishClickPort` and `ClickIdGenerator`, service `RegisterClickService`) are plain
Kotlin with no Spring/Kafka imports. A web adapter calls the inbound port; a Kafka adapter
implements the outbound port with `KafkaTemplate`, keyed by `adId`. Spring wiring lives in `config/`.

**Tech Stack:** Kotlin 2.4 / JDK 25, Spring Boot (latest stable 4.x), Spring for Apache Kafka,
Jackson 3 (Kotlin module), JUnit 5, MockK.

**Spec:** `docs/superpowers/specs/2026-11-06-click-service-design.md`

## Global Constraints

- Module path `services/click-service`, Gradle project `:services:click-service`, base package
  `com.wmbatista.adclick.clickservice`.
- Domain and application packages (`domain`, `application`) must not import Spring or Kafka types.
- Adapter/port package names: `application.port.inbound`, `application.port.outbound`,
  `adapter.inbound.web`, `adapter.outbound.kafka` (`in` is a Kotlin keyword).
- Endpoint: `POST /click`, JSON body `{"adId": "<non-blank string>"}`; 202 empty body on success;
  400 on missing/null/blank `adId` or malformed body; 503 when publishing fails or times out.
- Topic `click-events` (configurable), declared by the service: **12 partitions**, replication
  factor **1** (both configurable). Message key = `adId`.
- Wire contract: `ClickEvent(clickId: UUID, adId: String, timestamp: Instant)` in
  `libs/click-event`, JSON, `timestamp` as ISO-8601 UTC string, `clickId` as UUID string, no Spring
  type headers.
- Producer: `acks=all`, `enable.idempotence=true`; `delivery.timeout.ms` ≤ use-case send timeout and
  `delivery.timeout.ms` ≥ `linger.ms` + `request.timeout.ms`.
- `spring.threads.virtual.enabled=true`; bootstrap servers `${KAFKA_BOOTSTRAP_SERVERS:localhost:9092}`.
- Versions are declared only in `gradle/libs.versions.toml`. Tests are unit tests only — no
  Testcontainers, no test that needs a running broker.

## Review Focus

1. **Broker unreachable at request time** — `KafkaTemplate.send` throws synchronously (metadata
   wait exceeds `max.block.ms`); caller must get 503, not 500. Test in Task 3.
2. **Send never acknowledged** — the future does not complete within the send timeout; must become
   `ClickPublishingException` (→ 503), not hang. Test in Task 3.
3. **Bad request bodies** — missing `adId`, `null`, `""`, `"   "`, non-JSON body: 400 and the use
   case is never invoked (no event published). Tests in Task 5.
4. **Wire format drift** — the serializer configured in `application.yml` must produce
   `"timestamp":"2026-11-06T12:34:56Z"`-style strings, not epoch numbers, and no `__TypeId__`
   header the `stream-processor` would depend on. Test in Task 3.
5. **Thread interrupted while waiting for the ack** — interrupt flag restored and
   `ClickPublishingException` thrown. Test in Task 3.

---

### Task 1: Rename module and bootstrap Spring Boot

**Files:**
- Move: `services/click-processor/` → `services/click-service/`
- Delete: `services/click-service/src/main/kotlin/com/wmbatista/adclick/clickprocessor/Main.kt`
- Create: `services/click-service/src/main/kotlin/com/wmbatista/adclick/clickservice/ClickServiceApplication.kt`
- Create: `services/click-service/src/main/resources/application.yml`
- Create: `build-logic/src/main/kotlin/kotlin-spring-service-convention.gradle.kts`
- Modify: `build-logic/build.gradle.kts`, `gradle/libs.versions.toml`, `settings.gradle.kts`,
  `services/click-service/build.gradle.kts`, `README.md`

**Interfaces:**
- Produces: Gradle project `:services:click-service` applying `kotlin-spring-service-convention`;
  catalog aliases `spring-boot-dependencies` (BOM), `spring-boot-starter-webmvc`,
  `spring-boot-starter-kafka`, `spring-boot-starter-validation`, `jackson-module-kotlin`,
  `spring-boot-starter-test`, `mockk`, plus build-logic aliases `spring-boot-gradle-plugin` and
  `kotlin-allopen`.

- [ ] **Step 1: Look up versions.** On Maven Central, find the latest stable Spring Boot 4.x that
  supports Kotlin 2.4 / JDK 25 and the latest MockK. Confirm the Boot 4 starter artifact names
  above (Boot 4 split the starters; if `spring-boot-starter-webmvc`/`-kafka` don't exist in the
  chosen version, use `spring-boot-starter-web` / `spring-kafka`). Jackson Kotlin module is
  `tools.jackson.module:jackson-module-kotlin` (Jackson 3), version managed by the Boot BOM.
- [ ] **Step 2: Version catalog.** Add `spring-boot` and `mockk` versions and the aliases listed
  under Produces. Libraries managed by the BOM get no version.
- [ ] **Step 3: build-logic.** Add `spring-boot-gradle-plugin` and `kotlin-allopen` (version ref
  `kotlin`) to `build-logic/build.gradle.kts` dependencies. Create
  `kotlin-spring-service-convention.gradle.kts` applying `kotlin-service-convention`,
  `org.springframework.boot` and `org.jetbrains.kotlin.plugin.spring`, and adding
  `implementation(platform(spring-boot-dependencies))` plus
  `testImplementation(spring-boot-starter-test)` and `testImplementation(mockk)` via
  `versionCatalogs.named("libs")` (same pattern as `kotlin-jvm-convention`). Rationale: applying
  the Kotlin Spring plugin by alias in the module would clash with the Kotlin plugin already on the
  classpath from build-logic.
- [ ] **Step 4: Rename.** `git mv services/click-processor services/click-service`; update
  `settings.gradle.kts`; module build applies `kotlin-spring-service-convention`, depends on
  `:libs:click-event`, the webmvc/kafka/validation starters and `jackson-module-kotlin`, and sets
  `application.mainClass = "com.wmbatista.adclick.clickservice.ClickServiceApplicationKt"`.
- [ ] **Step 5: Application.** `@SpringBootApplication @ConfigurationPropertiesScan class
  ClickServiceApplication` + `fun main(args: Array<String>) = runApplication<ClickServiceApplication>(*args)`.
  `application.yml` for now: `spring.application.name: click-service`,
  `spring.threads.virtual.enabled: true`.
- [ ] **Step 6: README.** Replace `click-processor` with `click-service` in the architecture table,
  the repository layout and the `./gradlew :services:click-service:run` example.
- [ ] **Step 7: Verify.** Run `./gradlew build`. Expected: BUILD SUCCESSFUL;
  `grep -rn click-processor --exclude-dir=build --exclude-dir=.gradle .` finds nothing outside
  `docs/superpowers/`.
- [ ] **Step 8: Commit** — `git commit -m "Rename click-processor to click-service and bootstrap Spring Boot"`.

### Task 2: Domain and application layer

**Files:**
- Create under `services/click-service/src/main/kotlin/com/wmbatista/adclick/clickservice/`:
  `domain/Click.kt`, `application/port/inbound/RegisterClickUseCase.kt`,
  `application/port/outbound/PublishClickPort.kt`, `application/port/outbound/ClickIdGenerator.kt`,
  `application/port/outbound/ClickPublishingException.kt`,
  `application/service/RegisterClickService.kt`
- Test: `services/click-service/src/test/kotlin/com/wmbatista/adclick/clickservice/application/service/RegisterClickServiceTest.kt`

**Interfaces:**
- Produces:
  - `data class Click(val clickId: UUID, val adId: String, val timestamp: Instant)`
  - `interface RegisterClickUseCase { fun register(adId: String): Click }`
  - `interface PublishClickPort { fun publish(click: Click) }` — throws `ClickPublishingException`
  - `fun interface ClickIdGenerator { fun next(): UUID }`
  - `class ClickPublishingException(message: String, cause: Throwable? = null) : RuntimeException(message, cause)`
  - `class RegisterClickService(publishClickPort: PublishClickPort, clock: Clock, clickIdGenerator: ClickIdGenerator) : RegisterClickUseCase` — no Spring annotations.

- [ ] **Step 1: Write the failing tests** (fake port = in-memory list; fixed clock
  `Clock.fixed(Instant.parse("2026-11-06T12:34:56Z"), ZoneOffset.UTC)`; generator returning
  `UUID.fromString("00000000-0000-0000-0000-000000000001")`):
  - `register publishes click with generated id, given adId and current time` — returned `Click`
    equals `Click(uuid, "ad-42", Instant.parse("2026-11-06T12:34:56Z"))` and the fake received
    exactly that one click.
  - `register propagates publishing failure` — fake throws `ClickPublishingException`;
    `assertThrows<ClickPublishingException> { service.register("ad-42") }`.
- [ ] **Step 2: Run** `./gradlew :services:click-service:test --tests '*RegisterClickServiceTest'`.
  Expected: compilation failure (types missing).
- [ ] **Step 3: Implement** the types listed under Produces.
- [ ] **Step 4: Run** the same command. Expected: PASS.
- [ ] **Step 5: Commit** — `git commit -m "Add click domain model and register click use case"`.

### Task 3: Kafka outbound adapter and event contract

**Files:**
- Modify: `libs/click-event/src/main/kotlin/com/wmbatista/adclick/event/ClickEvent.kt`
- Create: `.../clickservice/adapter/outbound/kafka/ClickEventMapper.kt`,
  `.../clickservice/adapter/outbound/kafka/KafkaClickPublisher.kt`
- Test: `.../clickservice/adapter/outbound/kafka/KafkaClickPublisherTest.kt`,
  `.../clickservice/adapter/outbound/kafka/ClickEventSerializationTest.kt`

**Interfaces:**
- Consumes: `Click`, `PublishClickPort`, `ClickPublishingException` (Task 2).
- Produces:
  - `data class ClickEvent(val clickId: UUID, val adId: String, val timestamp: Instant)` (replaces
    the placeholder; keep a KDoc saying it is the `click-events` wire contract).
  - `fun Click.toClickEvent(): ClickEvent`
  - `class KafkaClickPublisher(kafkaTemplate: KafkaTemplate<String, ClickEvent>, topic: String, sendTimeout: Duration) : PublishClickPort`

- [ ] **Step 1: Write the failing tests** in `KafkaClickPublisherTest` (MockK `KafkaTemplate`,
  topic `"click-events"`, `click = Click(uuid, "ad-42", Instant.parse("2026-11-06T12:34:56Z"))`):
  - `publish sends event to topic keyed by adId` — stub `send(any<String>(), any(), any())` to
    return a completed `CompletableFuture<SendResult<String, ClickEvent>>`; verify
    `send("click-events", "ad-42", ClickEvent(uuid, "ad-42", Instant.parse("2026-11-06T12:34:56Z")))`.
  - `publish fails when broker rejects the record` — future completed exceptionally with
    `KafkaException("boom")` → `assertThrows<ClickPublishingException>`.
  - `publish fails when send throws synchronously` — `send` throws `KafkaException` →
    `ClickPublishingException`.
  - `publish fails when ack does not arrive within timeout` — never-completed future,
    `sendTimeout = Duration.ofMillis(50)` → `ClickPublishingException`.
  - `publish restores interrupt flag when interrupted` — `Thread.currentThread().interrupt()` before
    calling with a never-completed future → `ClickPublishingException` and
    `Thread.interrupted()` returns `true` afterwards.
- [ ] **Step 2: Write the failing test** `ClickEventSerializationTest`:
  - `click event serializes as iso timestamp and uuid string without type headers` — instantiate
    the Spring Kafka JSON serializer class that Task 4 will put in `application.yml` (Jackson 3:
    `org.springframework.kafka.support.serializer.JacksonJsonSerializer`; fall back to
    `JsonSerializer` only if the Spring Kafka version lacks it), `configure(mapOf("spring.json.add.type.headers" to false), false)`,
    serialize with a `RecordHeaders` instance, parse the bytes and assert
    `timestamp == "2026-11-06T12:34:56Z"`, `clickId == "00000000-0000-0000-0000-000000000001"`,
    `adId == "ad-42"`, and `headers.lastHeader("__TypeId__") == null`.
- [ ] **Step 3: Run** `./gradlew :services:click-service:test --tests '*kafka*'`. Expected: FAIL
  (compilation).
- [ ] **Step 4: Implement** `ClickEvent`, `toClickEvent`, and `KafkaClickPublisher.publish`: call
  `kafkaTemplate.send(topic, click.adId, click.toClickEvent())` and `get(sendTimeout)` on the
  future; wrap `ExecutionException`, `TimeoutException` and `KafkaException` (synchronous) in
  `ClickPublishingException`; on `InterruptedException` re-set the interrupt flag, then wrap.
- [ ] **Step 5: Run** the same command. Expected: PASS.
- [ ] **Step 6: Commit** — `git commit -m "Add Kafka click publisher and click event contract"`.

### Task 4: Wiring, topic declaration and producer configuration

**Files:**
- Create: `.../clickservice/config/ClickServiceProperties.kt`,
  `.../clickservice/config/ClickServiceConfiguration.kt`
- Modify: `services/click-service/src/main/resources/application.yml`
- Test: `.../clickservice/config/ProducerTimeoutConfigurationTest.kt`,
  `.../clickservice/config/ClickServiceConfigurationTest.kt`

**Interfaces:**
- Consumes: `RegisterClickService`, `ClickIdGenerator`, `PublishClickPort` (Task 2);
  `KafkaClickPublisher`, `ClickEvent` (Task 3).
- Produces:
  - `@ConfigurationProperties("click-service") data class ClickServiceProperties(val topic: Topic, val sendTimeout: Duration)`
    with nested `data class Topic(val name: String, val partitions: Int, val replicationFactor: Short)`.
  - `@Configuration class ClickServiceConfiguration` with beans: `clock(): Clock` (`Clock.systemUTC()`),
    `clickIdGenerator(): ClickIdGenerator` (`UUID::randomUUID`),
    `publishClickPort(kafkaTemplate: KafkaTemplate<String, ClickEvent>, properties): PublishClickPort`,
    `registerClickUseCase(...): RegisterClickUseCase`, `clickEventsTopic(properties): NewTopic`.

- [ ] **Step 1: Write the failing tests:**
  - `ProducerTimeoutConfigurationTest` loads `application.yml` with `YamlPropertiesFactoryBean`
    (no Spring context):
    - `delivery timeout does not exceed send timeout` —
      `delivery.timeout.ms <= Duration.parse/convert(click-service.send-timeout).toMillis()`.
    - `delivery timeout covers linger plus request timeout`.
    - `producer is durable and idempotent` — `acks == "all"`, `enable.idempotence == "true"`.
  - `ClickServiceConfigurationTest` (plain instantiation, no context):
    - `click events topic has configured partitions and replication` — with
      `Topic("click-events", 12, 1)` → `NewTopic.name() == "click-events"`, `numPartitions() == 12`,
      `replicationFactor() == 1.toShort()`.
- [ ] **Step 2: Run** `./gradlew :services:click-service:test --tests '*config*'`. Expected: FAIL.
- [ ] **Step 3: Configure `application.yml`** with these values:

```yaml
spring:
  application.name: click-service
  threads.virtual.enabled: true
  kafka:
    bootstrap-servers: ${KAFKA_BOOTSTRAP_SERVERS:localhost:9092}
    producer:
      acks: all
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: <same JSON serializer class used in ClickEventSerializationTest>
      properties:
        enable.idempotence: true
        linger.ms: 5
        request.timeout.ms: 3000
        delivery.timeout.ms: 5000
        max.block.ms: 1000
        spring.json.add.type.headers: false
click-service:
  send-timeout: 6s
  topic:
    name: click-events
    partitions: 12
    replication-factor: 1
```

- [ ] **Step 4: Implement** `ClickServiceProperties` and `ClickServiceConfiguration` (Produces).
- [ ] **Step 5: Run** the same command. Expected: PASS.
- [ ] **Step 6: Commit** — `git commit -m "Wire click service and configure Kafka producer and topic"`.

### Task 5: Web inbound adapter

**Files:**
- Create: `.../clickservice/adapter/inbound/web/ClickController.kt`,
  `.../clickservice/adapter/inbound/web/ClickRequest.kt`,
  `.../clickservice/adapter/inbound/web/ClickExceptionHandler.kt`
- Test: `.../clickservice/adapter/inbound/web/ClickControllerTest.kt`

**Interfaces:**
- Consumes: `RegisterClickUseCase`, `ClickPublishingException`, `Click` (Task 2).
- Produces:
  - `data class ClickRequest(@field:NotBlank val adId: String?)`
  - `@RestController class ClickController(registerClickUseCase: RegisterClickUseCase)` with
    `@PostMapping("/click") fun click(@Valid @RequestBody request: ClickRequest): ResponseEntity<Void>` → 202.
  - `@RestControllerAdvice class ClickExceptionHandler` mapping `ClickPublishingException` → 503
    (empty body).

- [ ] **Step 1: Write the failing tests** with standalone MockMvc
  (`MockMvcBuilders.standaloneSetup(ClickController(useCase)).setControllerAdvice(ClickExceptionHandler())`,
  ensure a JSON converter with the Kotlin module and a validator are registered) and a MockK
  `RegisterClickUseCase`:
  - `valid click returns 202` — body `{"adId":"ad-42"}` → 202, `verify(exactly = 1) { useCase.register("ad-42") }`.
  - `missing, null, empty or blank adId returns 400` — parameterized over `{}`,
    `{"adId":null}`, `{"adId":""}`, `{"adId":"   "}` → 400, `verify { useCase wasNot Called }`.
  - `malformed body returns 400` — body `not json` → 400, use case not called.
  - `publishing failure returns 503` — use case throws `ClickPublishingException("down")` → 503.
- [ ] **Step 2: Run** `./gradlew :services:click-service:test --tests '*ClickControllerTest'`.
  Expected: FAIL.
- [ ] **Step 3: Implement** the three classes (Produces).
- [ ] **Step 4: Run** the same command. Expected: PASS.
- [ ] **Step 5: Full verification.** `./gradlew build` → BUILD SUCCESSFUL, and
  `grep -rnE "import (org\.springframework|org\.apache\.kafka)" services/click-service/src/main/kotlin/com/wmbatista/adclick/clickservice/{domain,application}`
  returns nothing.
- [ ] **Step 6: Commit** — `git commit -m "Add POST /click web adapter"`.
