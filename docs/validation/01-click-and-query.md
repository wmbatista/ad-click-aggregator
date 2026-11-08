# 01 — Click and Query

Checks functional requirement 2 (advertisers query click metrics with 1-minute granularity) and the
part of requirement 1 that exists today (the click is recorded). The redirect to the advertiser's
website is **not implemented yet**: `POST /click` answers `202 Accepted` with no `Location`.

Also observed here: **near real-time** (clicks are queryable within about a second) and
**sub-second queries**.

## 1. A single click, by hand

```bash
curl -i -X POST localhost:8080/click -H 'Content-Type: application/json' -d '{"adId":"manual-1"}'
```

Expected: `HTTP/1.1 202`. A missing or blank `adId` answers `400`:

```bash
curl -s -o /dev/null -w '%{http_code}\n' -X POST localhost:8080/click \
  -H 'Content-Type: application/json' -d '{"adId":" "}'      # 400
```

Then query it as the advertiser:

```bash
scripts/query-metrics.py --ad manual-1 --last 5m
```

```
manual-1  MINUTE  2026-11-08T16:33:00Z .. 2026-11-08T16:39:00Z  (13 ms)
  2026-11-08T16:33:00Z         0
  ...
  2026-11-08T16:38:00Z         1  #
  total 1
```

Every minute of the range is listed, zeros included, so a chart can plot it directly. An ad that
never received a click is not an error: it returns all zeros.

## 2. Many ads, checked automatically

```bash
scripts/simulate-clicks.py --rate 200 --duration 5 --ads 5 --summary /tmp/run.json
scripts/query-metrics.py --summary /tmp/run.json
```

```
Sent 1000 requests (1000 clicks + 0 duplicates) in 5.0s: 200 requests/s
Responses: 202=1000
...
ad                                        accepted   w/o duplicates   counted  result
sim-20261108163802-ad-1                        207              207       207  ok
sim-20261108163802-ad-2                        190              190       190  ok
...
TOTAL                                         1000             1000      1000
Query latency over 5 queries: p50 7 ms, max 37 ms
Every ad matches the clicks accepted by the click service.
```

What this shows:

- **Accuracy:** every ad's count equals the clicks the click service accepted.
- **Near real-time:** the check passed on its first poll, run right after the simulation ended.
  The aggregator writes each Kafka poll (up to 500 events) as soon as it arrives, so the delay is
  well under a second at this rate. Use `--wait 0` to check once without polling.
- **Sub-second queries:** tens of milliseconds per query.

## 3. Other granularities and ranges

```bash
scripts/query-metrics.py --ad manual-1 --last 6h --granularity HOUR
scripts/query-metrics.py --ad manual-1 --last 7d --granularity DAY
```

The query service rejects bad ranges with `400`:

```bash
# from after to
curl -s -w ' %{http_code}\n' 'localhost:8081/ads/manual-1/clicks?from=2026-11-08T11:00:00Z&to=2026-11-08T10:00:00Z'
# more than 1,440 buckets (two days per minute)
curl -s -w ' %{http_code}\n' 'localhost:8081/ads/manual-1/clicks?from=2026-11-06T00:00:00Z&to=2026-11-08T00:00:00Z'
```

## Where the data lives

To see the same counts at each hop, use the [inspection toolbox](README.md#inspection-toolbox):
the click events in the `click-events` topic, then the per-minute rows in
`ad_clicks.clicks_per_minute`.
