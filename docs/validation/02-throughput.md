# 02 — Throughput

Checks the scalability target: a **peak of 10k clicks per second**, with every accepted click
counted and queries still sub-second.

```bash
scripts/simulate-clicks.py --rate 10000 --duration 10 --ads 100 --workers 200 --summary /tmp/load.json
scripts/query-metrics.py --summary /tmp/load.json
```

Result on a laptop (one container per service, one Kafka broker, one ClickHouse node):

```
Sent 100000 requests (100000 clicks + 0 duplicates) in 10.0s: 9968 requests/s
Responses: 202=100000
...
TOTAL                                       100000           100000    100000
Query latency over 100 queries: p50 3 ms, max 15 ms
Every ad matches the clicks accepted by the click service.
```

What to look at:

- **Achieved rate:** `requests/s` close to `--rate`. If it falls short, the limit is usually the
  simulator itself (one Python process); raise `--workers`, or run two simulators in parallel with
  different `--ad-prefix` values.
- **Responses:** only `202`. A `503` means a click was not stored (Kafka too slow or unreachable) and
  is excluded from the expected counts.
- **Aggregator lag** during the run, from a second shell:

  ```bash
  dc exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:29092 \
    --describe --group stream-processor
  ```

  `LAG` should stay small and return to `0` shortly after the run. A lag that keeps growing means
  the single aggregator instance cannot keep up; in production that is solved by running more
  instances (up to one per partition, 12 here).

## Caveats

This is a single-machine test: the simulator, the services, Kafka and ClickHouse share the same CPU.
It shows that the design handles the target rate without losing clicks, not how it would behave on
real hardware with network between the components.
