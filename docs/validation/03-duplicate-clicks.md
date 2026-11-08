# 03 — Duplicate Clicks

Checks the idempotency requirement: _the same click must not be counted more than once_.

**This requirement is not met yet** ([TD-1](../tech-debt.md)). This guide reproduces the gap so the
fix can be verified with the same steps: after TD-1 is paid, the `counted` column should equal
`w/o duplicates`.

## Duplicates sent by the browser

A duplicate here is a browser retry of the same click (for example after a timeout): the same
request reaches the click service twice.

```bash
scripts/simulate-clicks.py --rate 100 --duration 3 --ads 3 --duplicates 0.2 --summary /tmp/dup.json
scripts/query-metrics.py --summary /tmp/dup.json
```

```
Sent 359 requests (300 clicks + 59 duplicates) in 3.0s: 120 requests/s
Responses: 202=359
Accepted (202): 359, of which 59 were duplicates
...
ad                                        accepted   w/o duplicates   counted  result
sim-20261108163813-ad-1                        121              101       121  ok
sim-20261108163813-ad-2                        134              110       134  ok
sim-20261108163813-ad-3                        104               89       104  ok
TOTAL                                          359              300       359
Duplicates were sent and are counted like any other click (TD-1 in docs/tech-debt.md).
```

300 clicks were counted as 359. The click service gives every request a new `clickId`, so nothing
downstream can tell a retry from a genuine click. Telling them apart needs an id chosen by the
client (a signed impression id, for instance), which is one of the TD-1 options.

## Duplicates created by the pipeline

The aggregator commits its Kafka offsets only after the ClickHouse insert. If it dies between the
two, the batch is consumed again and inserted again, so its clicks are counted twice. Killing it
(no graceful shutdown) during a high-rate run reproduces this:

```bash
scripts/simulate-clicks.py --rate 5000 --duration 20 --ads 10 --workers 128 --summary /tmp/crash.json &
sleep 10; dc kill stream-processor; dc up -d --wait stream-processor
wait; scripts/query-metrics.py --summary /tmp/crash.json --wait 120
```

Observed:

```
sim-20261108164227-ad-1                       9968             9968     10005  over-counted by 37
sim-20261108164227-ad-10                      9790             9790      9815  over-counted by 25
...
TOTAL                                       100000           100000    100311
10 ads do not match the accepted clicks.
```

No click was lost, but 311 were counted twice: the batches that were in ClickHouse but not yet
committed in Kafka when the process died were replayed after the restart.

The counts take a while to catch up after a kill: the dead consumer never left the group, so Kafka
waits for its session to expire (`session.timeout.ms`, 45 s) before giving its partitions to the
restarted instance. That is why this check uses `--wait 120`. A graceful `dc stop` leaves the
group at once and does not have this delay.
