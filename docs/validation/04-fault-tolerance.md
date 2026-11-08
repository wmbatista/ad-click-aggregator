# 04 — Fault Tolerance

Checks that **no click is lost** when a component goes down. A click is "stored" once the click
service answers `202`; from then on it must reach the metrics, however late.

Uses the `dc` shortcut from the [inspection toolbox](README.md#inspection-toolbox).

## Aggregator down

Clicks keep being accepted (they wait in Kafka) and are counted when the aggregator returns.

```bash
dc stop stream-processor
scripts/simulate-clicks.py --rate 100 --duration 3 --ads 3 --summary /tmp/a.json
scripts/query-metrics.py --summary /tmp/a.json --wait 3        # fails: missing clicks
dc exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:29092 \
  --describe --group stream-processor                         # LAG > 0 on the run's partitions
dc start stream-processor
scripts/query-metrics.py --summary /tmp/a.json                 # passes
```

Observed:

```
sim-20261108163841-ad-3                        103              103         0  missing 103
TOTAL                                          300              300         0

GROUP            TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
stream-processor click-events    7          1366            1469            103
stream-processor click-events    8          2057            2157            100
stream-processor click-events    11         2213            2310            97

# after the restart
TOTAL                                          300              300       300
Every ad matches the clicks accepted by the click service.
```

The lag per partition equals the clicks per ad, since each ad maps to one partition.

A crash (`dc kill` instead of `dc stop`) also loses nothing, but it can count some clicks twice and
the restarted aggregator only resumes after about 45 s; see
[03 — Duplicates created by the pipeline](03-duplicate-clicks.md#duplicates-created-by-the-pipeline).

## ClickHouse down

Clicks keep being accepted. The aggregator retries each failed batch with backoff (500 ms doubling
up to 30 s) without committing offsets, and the query service answers `503`.

```bash
dc stop clickhouse
scripts/simulate-clicks.py --rate 50 --duration 2 --ads 2 --summary /tmp/b.json
curl -s -o /dev/null -w '%{http_code}\n' \
  'localhost:8081/ads/x/clicks?from=2026-11-08T10:00:00Z&to=2026-11-08T10:01:00Z'   # 503
dc logs --since 1m stream-processor | grep 'Batch insert failed'
curl -s localhost:8082/actuator/health                                              # DOWN
dc start clickhouse
scripts/query-metrics.py --summary /tmp/b.json --wait 90
```

Observed:

```
Responses: 202=100
503
WARN ... StreamProcessorConfiguration : Batch insert failed (attempt 1, 1 records from [click-events-1]); offsets not committed, retrying
...
TOTAL                                          100              100       100
Every ad matches the clicks accepted by the click service.
```

The check keeps polling through the `503`s and passes once ClickHouse is back and the next retry
writes the pending batches. Because the backoff grows to 30 s, the counts can take up to that long
to appear after ClickHouse recovers.

## Kafka down

The click service cannot store clicks, so it refuses them with `503` (in about a second) instead of
answering `202` for clicks it would lose. The caller may retry.

```bash
dc stop kafka
curl -s -o /dev/null -w '%{http_code}\n' -X POST localhost:8080/click \
  -H 'Content-Type: application/json' -d '{"adId":"k"}'                # 503
scripts/simulate-clicks.py --rate 5 --duration 2 --ads 1               # Responses: 503=10
dc start kafka && dc up -d --wait
scripts/simulate-clicks.py --rate 50 --duration 2 --ads 2 --summary /tmp/c.json
scripts/query-metrics.py --summary /tmp/c.json                         # passes
```

Every service recovers on its own once Kafka is healthy again; no restart is needed.

A narrow window remains in which a click answered with `503` was in fact written by Kafka, and its
retry is then counted twice. See the click service section of [architecture](../architecture.md)
and TD-1.
