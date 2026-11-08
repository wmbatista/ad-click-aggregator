#!/usr/bin/env python3
"""Queries click metrics as an advertiser, or checks a simulation run against the metrics.

Advertiser mode, one ad:
  scripts/query-metrics.py --ad ad-1                          # last 15 minutes, per minute
  scripts/query-metrics.py --ad ad-1 --last 6h --granularity HOUR
  scripts/query-metrics.py --ad ad-1 --from 2026-11-08T10:00:00Z --to 2026-11-08T11:00:00Z

Check mode, every ad of a run of scripts/simulate-clicks.py --summary <file>:
  scripts/query-metrics.py --summary run.json                 # waits up to 30 s for the counts
  scripts/query-metrics.py --summary run.json --wait 0        # check once

Check mode compares each ad's total with the clicks the click service accepted (202) and exits
with status 0 only when every ad matches. Each query's latency is reported, since the metrics
API should answer in under a second.

Only the Python standard library is used (Python 3.9+).
"""
import argparse
import json
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone

UNITS = {"m": "minutes", "h": "hours", "d": "days"}
STEP = {"MINUTE": timedelta(minutes=1), "HOUR": timedelta(hours=1), "DAY": timedelta(days=1)}


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--url", default="http://localhost:8081", help="query service base URL")
    target = parser.add_mutually_exclusive_group(required=True)
    target.add_argument("--ad", help="ad id to query")
    target.add_argument("--summary", help="summary JSON written by simulate-clicks.py --summary")
    parser.add_argument("--from", dest="from_", help="ISO-8601 UTC start, inclusive")
    parser.add_argument("--to", help="ISO-8601 UTC end, exclusive (default: end of the current bucket)")
    parser.add_argument("--last", default="15m", help="range ending now when --from is absent, e.g. 30m, 6h, 1d")
    parser.add_argument("--granularity", default="MINUTE", choices=sorted(STEP), type=str.upper)
    parser.add_argument("--wait", type=float, default=30,
                        help="check mode: seconds to keep polling until the counts match (default 30)")
    return parser.parse_args()


def iso(moment):
    return moment.strftime("%Y-%m-%dT%H:%M:%SZ")


def query(base_url, ad_id, from_, to, granularity):
    """Returns (status, body or error text, latency in ms)."""
    params = urllib.parse.urlencode({"from": from_, "to": to, "granularity": granularity})
    url = "%s/ads/%s/clicks?%s" % (base_url, urllib.parse.quote(ad_id, safe=""), params)
    started = time.monotonic()
    try:
        with urllib.request.urlopen(url, timeout=10) as response:
            status, body = response.status, json.load(response)
    except urllib.error.HTTPError as error:
        status, body = error.code, error.read().decode() or error.reason
    except (urllib.error.URLError, OSError) as error:
        status, body = None, str(error)
    return status, body, (time.monotonic() - started) * 1000


def advertiser_mode(args):
    if args.from_:
        from_, to = args.from_, args.to or iso(datetime.now(timezone.utc) + STEP[args.granularity])
    else:
        match = re.fullmatch(r"(\d+)([mhd])", args.last)
        if not match:
            sys.exit("--last must look like 30m, 6h or 1d")
        now = datetime.now(timezone.utc)
        # The service rounds `to` up to the granularity, so the current bucket is included.
        from_ = iso(now - timedelta(**{UNITS[match.group(2)]: int(match.group(1))}))
        to = args.to or iso(now)
    status, body, latency = query(args.url, args.ad, from_, to, args.granularity)
    if status != 200:
        sys.exit("query failed (%s, %.0f ms): %s" % (status, latency, body))
    print("%s  %s  %s .. %s  (%.0f ms)" % (body["adId"], body["granularity"], body["from"], body["to"], latency))
    for bucket in body["buckets"]:
        print("  %s  %8d  %s" % (bucket["start"], bucket["count"], "#" * min(bucket["count"], 60)))
    print("  total %d" % body["total"])


def check_mode(args):
    with open(args.summary) as source:
        run = json.load(source)
    ads = run["ads"]
    deadline = time.monotonic() + args.wait
    while True:
        rows, latencies, failure = [], [], None
        for ad_id, expected in sorted(ads.items()):
            status, body, latency = query(args.url, ad_id, run["from"], run["to"], "MINUTE")
            latencies.append(latency)
            if status != 200:
                # The query service answers 503 while ClickHouse is down: keep waiting for it.
                failure = "query for %s failed (%s): %s" % (ad_id, status, body)
                break
            rows.append((ad_id, expected["accepted"], expected["acceptedWithoutDuplicates"], body["total"]))
        done = failure is None and all(actual == accepted for _, accepted, _, actual in rows)
        if done or time.monotonic() >= deadline:
            break
        time.sleep(1)
    if failure:
        sys.exit(failure)

    print("Run %s, window %s .. %s" % (run["prefix"], run["from"], run["to"]))
    print("%-40s %9s %16s %9s  %s" % ("ad", "accepted", "w/o duplicates", "counted", "result"))
    mismatches = 0
    for ad_id, accepted, unique, actual in rows:
        if actual == accepted:
            result = "ok"
        elif actual > accepted:
            result = "over-counted by %d" % (actual - accepted)
        else:
            result = "missing %d" % (accepted - actual)
        mismatches += actual != accepted
        print("%-40s %9d %16d %9d  %s" % (ad_id, accepted, unique, actual, result))
    total = sum(row[3] for row in rows)
    print("%-40s %9d %16d %9d" % ("TOTAL", run["expectedTotal"], run["expectedTotalWithoutDuplicates"], total))
    latencies.sort()
    print("Query latency over %d queries: p50 %.0f ms, max %.0f ms"
          % (len(latencies), latencies[len(latencies) // 2], latencies[-1]))
    if run["expectedTotal"] != run["expectedTotalWithoutDuplicates"]:
        print("Duplicates were sent and are counted like any other click (TD-1 in docs/tech-debt.md).")
    if mismatches:
        print("%d ads do not match the accepted clicks." % mismatches, file=sys.stderr)
        sys.exit(1)
    print("Every ad matches the clicks accepted by the click service.")


def main():
    args = parse_args()
    if args.summary:
        check_mode(args)
    else:
        advertiser_mode(args)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
