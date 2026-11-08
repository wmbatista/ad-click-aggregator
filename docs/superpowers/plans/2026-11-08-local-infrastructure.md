# Local Infrastructure Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** One command (`scripts/bootstrap.sh`) brings up Kafka, ClickHouse and the click service locally, with the `click-events` topic and the ClickHouse schema in place, so a `POST /click` lands in Kafka.

**Architecture:** A single Docker Compose file in `infra/` runs a single-node KRaft Kafka, a one-shot topic-init container, ClickHouse with the shared schema mounted as an init script, and the click service built from a shared Dockerfile that packages a jar built on the host. Every long-running container has a healthcheck, so `docker compose up --wait` returns only when the stack is usable.

**Tech Stack:** Docker Compose v2, `apache/kafka:4.2.2`, `clickhouse/clickhouse-server:26.8.36.3`, `eclipse-temurin:25-jre`, Bash, Spring Boot Actuator.

**Spec:** `docs/superpowers/specs/2026-11-08-shared-contracts-design.md` (work item **A**).

## Global Constraints

- Images: Kafka `apache/kafka:4.2.2`; ClickHouse `clickhouse/clickhouse-server:26.8.36.3`; service base `eclipse-temurin:25-jre`.
- Topic `click-events`: 12 partitions, replication factor 1, created by a one-shot init container; broker topic auto-creation disabled.
- Kafka listeners: `kafka:29092` inside Compose, `localhost:9092` from the host.
- ClickHouse: user `ad_clicks`, password `ad_clicks`, database `ad_clicks`; ports 8123 (HTTP) and 9000 (native); schema from `infra/clickhouse/init/001-schema.sql` (already committed, do not change it).
- click-service: host port 8080, env `KAFKA_BOOTSTRAP_SERVERS=kafka:29092`, healthcheck on `GET /actuator/health`.
- Service jars are built on the host with `./gradlew bootJar`; one shared `infra/docker/service.Dockerfile` takes the jar path as a build arg.
- File ownership: only `infra/`, `scripts/`, and `services/click-service` (Actuator only). Do not edit `services/stream-processor`, `services/query-service`, `gradle/libs.versions.toml`, `README.md` or `docs/architecture.md`.

**Deviation from the spec, decided while planning:** stream-processor and query-service are still placeholders with no Spring Boot app and no `bootJar`, so their Compose entries are **not** part of this plan. They are added in the integration step, once B and C are merged. The Compose file and Dockerfile are written so adding them is a copy of the click-service block with a different jar, port and env.

## Prerequisite (human, before Task 1)

No container runtime is installed on the development machine (`docker` is not on the PATH). Install one that provides the `docker` CLI with Compose v2 (Docker Desktop, OrbStack or Colima + `docker` + `docker-compose` plugin), then confirm:

Run: `docker version --format '{{.Server.Version}}' && docker compose version`
Expected: a server version and `Docker Compose version v2.x`.

The same runtime is required by the Testcontainers tests of work items B and C.

## Review Focus

1. **Re-running bootstrap on an existing stack** — the topic already exists and ClickHouse data volume is initialized; expected: `bootstrap.sh` succeeds again without errors (`--if-not-exists`, init scripts only run on an empty volume, and `CREATE ... IF NOT EXISTS`). Pinned in Task 3, Step 5.
2. **Kafka reachable from both sides** — a client on the host (`localhost:9092`) and a container (`kafka:29092`) must both get metadata pointing to an address they can reach; a wrong advertised listener only shows up as timeouts. Pinned in Task 2, Step 4 (host-side check) and Task 3, Step 4 (container-side, via click-service).
3. **Click service starting before the topic exists** — expected: it starts only after `kafka-init` completed successfully (`depends_on: condition: service_completed_successfully`). Pinned in Task 3, Step 4 (`up --wait` from scratch after `reset`).
4. **Stale jar** — bootstrap must always rebuild jars and images, so a code change is never silently ignored. Pinned in Task 3, Step 3 (`bootJar` then `up --build`).
5. **Missing Docker or Compose v2** — expected: `bootstrap.sh` exits non-zero with a clear message instead of a cryptic shell error. Pinned in Task 3, Step 6.

---

### Task 1: Actuator health on click-service

**Files:**
- Modify: `services/click-service/build.gradle.kts` (add dependency)
- Modify: `services/click-service/src/main/resources/application.yml` (management config)

**Interfaces:**
- Produces: `GET http://<host>:8080/actuator/health` → `200` with body containing `"status":"UP"`. Only the `health` endpoint is exposed over HTTP.

- [ ] **Step 1: Add `implementation(libs.spring.boot.starter.actuator)` to `services/click-service/build.gradle.kts`.**

- [ ] **Step 2: In `application.yml`, set `management.endpoints.web.exposure.include: health`** (explicit, so later additions are deliberate) and `server.port: ${SERVER_PORT:8080}`.

- [ ] **Step 3: Build and run the existing tests**

Run: `./gradlew :services:click-service:build`
Expected: `BUILD SUCCESSFUL`, all existing tests pass.

- [ ] **Step 4: Commit**

```bash
git add services/click-service/build.gradle.kts services/click-service/src/main/resources/application.yml
git commit -m "Expose Actuator health on click service"
```

The health endpoint itself is verified end to end by the Compose healthcheck in Task 3; a Spring context test would need a Kafka broker and adds nothing over that.

---

### Task 2: Kafka and ClickHouse in Compose

**Files:**
- Create: `infra/compose.yaml`

**Interfaces:**
- Produces (Compose project name `ad-click-aggregator`, set with top-level `name:`):
  - service `kafka`: listeners `PLAINTEXT://:29092` (advertised `kafka:29092`), `PLAINTEXT_HOST://:9092` (advertised `localhost:9092`, published as host 9092), `CONTROLLER://:9093`; healthy when `/opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:29092` succeeds.
  - service `kafka-init`: same image, runs once, exits 0 after the topic exists.
  - service `clickhouse`: ports 8123 and 9000 published; healthy when `clickhouse-client --user ad_clicks --password ad_clicks -q 'SELECT 1'` succeeds.
  - named volumes `kafka-data` and `clickhouse-data`.

- [ ] **Step 1: Write the `kafka` service.** Single-node KRaft via the image's env vars: `KAFKA_NODE_ID=1`, `KAFKA_PROCESS_ROLES=broker,controller`, `KAFKA_CONTROLLER_QUORUM_VOTERS=1@kafka:9093`, `KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER`, `KAFKA_LISTENERS`, `KAFKA_ADVERTISED_LISTENERS` and `KAFKA_LISTENER_SECURITY_PROTOCOL_MAP` per the Interfaces block, `KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT`, `KAFKA_AUTO_CREATE_TOPICS_ENABLE=false`, and replication factor / min ISR 1 for the offsets and transaction-state topics. Data on volume `kafka-data` at `/var/lib/kafka/data` (set `KAFKA_LOG_DIRS` to it).

- [ ] **Step 2: Write the `kafka-init` service.** `depends_on: kafka: condition: service_healthy`, `restart: "no"`, command:

```sh
/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --create --if-not-exists \
  --topic click-events --partitions 12 --replication-factor 1
```

- [ ] **Step 3: Write the `clickhouse` service.** Env `CLICKHOUSE_USER=ad_clicks`, `CLICKHOUSE_PASSWORD=ad_clicks`, `CLICKHOUSE_DB=ad_clicks`, `CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1`; mount `./clickhouse/init` read-only at `/docker-entrypoint-initdb.d`; volume `clickhouse-data` at `/var/lib/clickhouse`; `ulimits.nofile` 262144 soft and hard.

- [ ] **Step 4: Bring the infrastructure up and verify it from the host**

Run: `docker compose -f infra/compose.yaml up -d --wait kafka kafka-init clickhouse`
Expected: exits 0; `docker compose -f infra/compose.yaml ps -a` shows `kafka` and `clickhouse` healthy and `kafka-init` exited (0).

Run: `docker compose -f infra/compose.yaml exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic click-events`
Expected: `PartitionCount: 12` and `ReplicationFactor: 1` (this goes through the host listener's advertised address).

Run: `curl -s 'http://localhost:8123/?user=ad_clicks&password=ad_clicks' --data-binary 'SHOW CREATE TABLE ad_clicks.clicks_per_minute'`
Expected: DDL containing `SummingMergeTree(click_count)` and `ORDER BY (ad_id, minute)`.

- [ ] **Step 5: Commit**

```bash
git add infra/compose.yaml
git commit -m "Add Kafka and ClickHouse to local Compose"
```

---

### Task 3: Click service container and bootstrap script

**Files:**
- Create: `infra/docker/service.Dockerfile`
- Create: `infra/docker/service.Dockerfile.dockerignore`
- Modify: `infra/compose.yaml` (add `click-service`)
- Create: `scripts/bootstrap.sh` (executable)
- Delete: `scripts/.gitkeep`

**Interfaces:**
- Consumes: Task 1 health endpoint; Task 2 services `kafka`, `kafka-init`, `clickhouse`.
- Produces:
  - `infra/docker/service.Dockerfile` with build arg `JAR_FILE` (path relative to the repo root); build context is the repo root.
  - Compose service `click-service`: build context `..`, dockerfile `infra/docker/service.Dockerfile`, `JAR_FILE=services/click-service/build/libs/click-service.jar`, port `8080:8080`, env `KAFKA_BOOTSTRAP_SERVERS=kafka:29092`, `depends_on: kafka-init: condition: service_completed_successfully`, healthcheck on `/actuator/health`.
  - `scripts/bootstrap.sh [up|down|reset]` — `up` (default) builds jars and starts everything, waiting for health; `down` stops containers and keeps data; `reset` stops and deletes volumes. Works from any current directory.

- [ ] **Step 1: Write the Dockerfile.** `FROM eclipse-temurin:25-jre`, `ARG JAR_FILE`, copy it to `/app/app.jar`, run as a non-root user, `ENTRYPOINT ["java", "-jar", "/app/app.jar"]`. The healthcheck needs an HTTP client in the image: check whether `curl` exists in the base image (`docker run --rm eclipse-temurin:25-jre which curl`) and install it with `apt-get` only if it does not.

- [ ] **Step 2: Write the Dockerfile-specific ignore file** so the repo-root context only ships jars: ignore everything (`*`), then re-include `services/*/build/libs/*.jar`.

- [ ] **Step 3: Add `click-service` to `infra/compose.yaml`** per the Interfaces block, and write `scripts/bootstrap.sh`: `set -euo pipefail`, resolve the repo root from the script's own location, `up` runs `./gradlew bootJar` then `docker compose -f infra/compose.yaml up -d --build --wait` and prints the endpoints (click service, Kafka, ClickHouse HTTP); `down` runs `docker compose ... down`; `reset` runs `docker compose ... down -v`; any other argument prints usage and exits 2.

- [ ] **Step 4: End-to-end check from scratch**

Run: `scripts/bootstrap.sh reset && scripts/bootstrap.sh`
Expected: exits 0 with all services healthy.

Run: `curl -s -o /dev/null -w '%{http_code}\n' -X POST localhost:8080/click -H 'Content-Type: application/json' -d '{"adId":"ad-1"}'`
Expected: `202`.

Run: `docker compose -f infra/compose.yaml exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:29092 --topic click-events --from-beginning --max-messages 1 --property print.key=true`
Expected: one line with key `ad-1` and a JSON value with `clickId`, `adId":"ad-1"` and an ISO-8601 `timestamp`.

- [ ] **Step 5: Re-run on an existing stack**

Run: `scripts/bootstrap.sh`
Expected: exits 0 again; `kafka-init` exits 0 (topic already exists); the click from Step 4 is still in the topic.

- [ ] **Step 6: Missing prerequisites fail clearly.** Add a check at the top of `bootstrap.sh` that `docker` is on the PATH and `docker compose version` succeeds; otherwise print `docker with the Compose v2 plugin is required` to stderr and exit 1.

Run: `PATH=/usr/bin:/bin scripts/bootstrap.sh; echo "exit=$?"`
Expected: the message above and `exit=1` (assuming `docker` is not in `/usr/bin` or `/bin`).

- [ ] **Step 7: Clean up and commit**

Run: `scripts/bootstrap.sh down`

```bash
git rm scripts/.gitkeep
git add infra/docker infra/compose.yaml scripts/bootstrap.sh
git commit -m "Run click service in Compose and add bootstrap script"
```
