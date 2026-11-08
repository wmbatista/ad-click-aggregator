# Validation Guides

Step-by-step checks that the running system does what the [architecture](../architecture.md)
promises. Each guide triggers a use case with the simulation scripts, says what to look at, and shows
the output we got when it was written (2026-11-08, Docker Desktop on an Apple Silicon Mac).

| Guide | Requirement checked |
|-------|---------------------|
| [01 — Click and query](01-click-and-query.md) | Clicks are recorded and advertisers can query them per minute, hour or day; near real-time; sub-second queries |
| [02 — Throughput](02-throughput.md) | Peak of 10k clicks per second without losing clicks |
| [03 — Duplicate clicks](03-duplicate-clicks.md) | Idempotency (currently **not met**, TD-1) |
| [04 — Fault tolerance](04-fault-tolerance.md) | No click lost when the aggregator, ClickHouse or Kafka go down |

## Prerequisites

- The stack is up: `scripts/stack.sh` (see the [README](../../README.md#running-the-local-stack)).
- Python 3.9+ for the simulation scripts (macOS ships one; no packages needed).

## Simulation scripts

| Script | What it does |
|--------|--------------|
| `scripts/simulate-clicks.py` | Sends `POST /click` at a steady rate over a set of fresh ad ids, optionally sending a fraction of clicks twice. `--summary run.json` records how many clicks each ad got accepted. |
| `scripts/query-metrics.py --ad <id>` | Queries one ad's metrics as an advertiser and prints the buckets. |
| `scripts/query-metrics.py --summary run.json` | Queries every ad of a simulation run, compares the totals with the accepted clicks, and exits with `0` only if they all match. It keeps polling (default 30 s, `--wait`) while counts are in flight or the query service answers `503`. |

Run either script with `--help` for every option. Each simulation uses new ad ids (prefix
`sim-<timestamp>`), so runs never interfere with each other or with older data.

## Inspection toolbox

The guides use these commands to look inside the pipeline. Define the shortcut once per shell:

```bash
dc() { docker compose -f infra/compose.yaml "$@"; }
```

**ClickHouse** — rows as stored. `rows` above 1 means `SummingMergeTree` has not merged the inserts yet;
that is expected, and is why every read must use `sum(click_count)`.

```bash
dc exec clickhouse clickhouse-client --user ad_clicks --password ad_clicks -d ad_clicks -q "
  SELECT ad_id, minute, count() AS rows, sum(click_count) AS clicks
  FROM clicks_per_minute WHERE ad_id LIKE 'sim-20261108163802-%'
  GROUP BY ad_id, minute ORDER BY ad_id, minute FORMAT PrettyCompact"
```

```
   ┌─ad_id───────────────────┬──────────────minute─┬─rows─┬─clicks─┐
1. │ sim-20261108163802-ad-1 │ 2026-11-08 16:38:00 │    1 │    207 │
2. │ sim-20261108163802-ad-2 │ 2026-11-08 16:38:00 │    1 │    190 │
   ...
```

**Kafka** — the raw click events, and how far behind the aggregator is (`LAG` is the number of click
events not yet written to ClickHouse):

```bash
dc exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:29092 \
  --topic click-events --from-beginning --max-messages 3 \
  --formatter-property print.key=true --formatter-property print.partition=true

dc exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:29092 \
  --describe --group stream-processor
```

```
Partition:0	sim-20261108163813-ad-1	{"clickId":"ef2b42ec-...","adId":"sim-20261108163813-ad-1","timestamp":"2026-11-08T16:38:13.276243424Z"}
```

Every event of an ad lands on the same partition, because the message key is the `adId`.

**Logs and health**

```bash
dc logs -f stream-processor          # batch retries appear as "Batch insert failed (attempt N, ...)"
dc ps                                # health of every container
curl -s localhost:8082/actuator/health   # stream processor; DOWN while ClickHouse is unreachable
```
