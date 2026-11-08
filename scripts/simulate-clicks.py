#!/usr/bin/env python3
"""Simulates users clicking ads: sends POST /click to the click service at a steady rate.

Every run uses fresh ad ids (prefixed with the run id), so its counts can be checked in isolation
with scripts/query-metrics.py --summary <file>.

Examples:
  scripts/simulate-clicks.py                                  # 100 clicks/s for 10 s over 10 ads
  scripts/simulate-clicks.py --rate 1000 --duration 30 --ads 50
  scripts/simulate-clicks.py --duplicates 0.2                 # 20% of clicks are sent twice

A duplicate is a browser retry of the same click: the request is sent a second time. The click
service cannot tell it apart from a new click today (see TD-1 in docs/tech-debt.md), so it is
counted twice; the summary reports the count with and without duplicates.

Only the Python standard library is used (Python 3.9+).
"""
import argparse
import http.client
import json
import queue
import random
import sys
import threading
import time
from collections import Counter
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--url", default="http://localhost:8080", help="click service base URL")
    parser.add_argument("--rate", type=float, default=100, help="target clicks per second (default 100)")
    parser.add_argument("--duration", type=float, default=10, help="seconds to send clicks for (default 10)")
    parser.add_argument("--ads", type=int, default=10, help="number of distinct ads (default 10)")
    parser.add_argument("--ad-prefix", help="ad id prefix (default: sim-<run timestamp>)")
    parser.add_argument("--duplicates", type=float, default=0.0,
                        help="fraction of clicks sent a second time, 0..1 (default 0)")
    parser.add_argument("--workers", type=int, default=32, help="concurrent HTTP connections (default 32)")
    parser.add_argument("--summary", help="write the run summary as JSON to this file")
    parser.add_argument("--seed", type=int, help="random seed, for reproducible ad choice")
    args = parser.parse_args()
    if args.rate <= 0 or args.duration <= 0 or args.ads <= 0 or args.workers <= 0:
        parser.error("--rate, --duration, --ads and --workers must be positive")
    if not 0 <= args.duplicates <= 1:
        parser.error("--duplicates must be between 0 and 1")
    return args


class Worker(threading.Thread):
    """Sends queued clicks over one keep-alive connection and records each response."""

    def __init__(self, base_url, jobs, results):
        super().__init__(daemon=True)
        url = urlparse(base_url)
        self.host, self.port = url.hostname, url.port or 80
        self.jobs, self.results = jobs, results
        self.conn = None

    def run(self):
        while True:
            job = self.jobs.get()
            if job is None:
                return
            ad_id, is_duplicate = job
            self.results.put((ad_id, is_duplicate, self.post(ad_id)))

    def post(self, ad_id):
        body = json.dumps({"adId": ad_id})
        for attempt in range(2):  # one reconnect if the server closed an idle keep-alive connection
            try:
                if self.conn is None:
                    self.conn = http.client.HTTPConnection(self.host, self.port, timeout=10)
                self.conn.request("POST", "/click", body, {"Content-Type": "application/json"})
                response = self.conn.getresponse()
                response.read()
                return response.status
            except (http.client.HTTPException, OSError) as error:
                self.conn.close()
                self.conn = None
                if attempt == 1:
                    return type(error).__name__
        return "unreachable"


def minute_floor(moment):
    return moment.replace(second=0, microsecond=0)


def iso(moment):
    return moment.strftime("%Y-%m-%dT%H:%M:%SZ")


def main():
    args = parse_args()
    rng = random.Random(args.seed)
    started_at = datetime.now(timezone.utc)
    prefix = args.ad_prefix or "sim-" + started_at.strftime("%Y%m%d%H%M%S")
    ad_ids = ["%s-ad-%d" % (prefix, n) for n in range(1, args.ads + 1)]

    jobs, results = queue.Queue(maxsize=args.workers * 4), queue.Queue()
    workers = [Worker(args.url, jobs, results) for _ in range(args.workers)]
    for worker in workers:
        worker.start()

    print("Sending ~%d clicks/s for %gs over %d ads (prefix %s), duplicates %.0f%%"
          % (args.rate, args.duration, args.ads, prefix, args.duplicates * 100))

    # Schedule click n at start + n / rate; a slow server makes the queue push back instead of
    # letting the backlog grow without bound, so the achieved rate is reported separately.
    total_clicks = int(args.rate * args.duration)
    start = time.monotonic()
    for n in range(total_clicks):
        delay = start + n / args.rate - time.monotonic()
        if delay > 0:
            time.sleep(delay)
        ad_id = rng.choice(ad_ids)
        jobs.put((ad_id, False))
        if rng.random() < args.duplicates:
            jobs.put((ad_id, True))
    for _ in workers:
        jobs.put(None)
    for worker in workers:
        worker.join()
    elapsed = time.monotonic() - start
    finished_at = datetime.now(timezone.utc)

    statuses = Counter()
    accepted_unique, accepted_duplicates = Counter(), Counter()
    while not results.empty():
        ad_id, is_duplicate, status = results.get()
        statuses[status] += 1
        if status == 202:
            (accepted_duplicates if is_duplicate else accepted_unique)[ad_id] += 1

    sent = sum(statuses.values())
    accepted = statuses[202]
    summary = {
        "prefix": prefix,
        "startedAt": iso(started_at),
        "finishedAt": iso(finished_at),
        # Window that contains every click's minute bucket, ready for the query service.
        "from": iso(minute_floor(started_at)),
        "to": iso(minute_floor(finished_at) + timedelta(minutes=1)),
        "requestsSent": sent,
        "duplicatesSent": sent - total_clicks,
        "statuses": {str(k): v for k, v in sorted(statuses.items(), key=lambda kv: str(kv[0]))},
        "achievedRate": round(sent / elapsed, 1),
        "ads": {
            ad_id: {
                "accepted": accepted_unique[ad_id] + accepted_duplicates[ad_id],
                "acceptedWithoutDuplicates": accepted_unique[ad_id],
            }
            for ad_id in ad_ids
        },
    }
    summary["expectedTotal"] = accepted
    summary["expectedTotalWithoutDuplicates"] = sum(accepted_unique.values())

    print("Sent %d requests (%d clicks + %d duplicates) in %.1fs: %.0f requests/s"
          % (sent, total_clicks, summary["duplicatesSent"], elapsed, summary["achievedRate"]))
    print("Responses: " + ", ".join("%s=%d" % (k, v) for k, v in summary["statuses"].items()))
    print("Accepted (202): %d, of which %d were duplicates" % (accepted, sum(accepted_duplicates.values())))
    print("Ads: %s-ad-1 .. %s-ad-%d (prefix %s)" % (prefix, prefix, args.ads, prefix))
    print("Window: from=%s to=%s" % (summary["from"], summary["to"]))
    print("Query one ad:   scripts/query-metrics.py --ad %s --from %s --to %s"
          % (ad_ids[0], summary["from"], summary["to"]))
    print("In ClickHouse:  ... WHERE ad_id LIKE '%s-%%'" % prefix)
    if args.summary:
        with open(args.summary, "w") as out:
            json.dump(summary, out, indent=2)
        print("Summary written to %s; check it with:" % args.summary)
        print("  scripts/query-metrics.py --summary %s" % args.summary)
    if accepted < sent:
        print("Some clicks were not accepted; they are not expected in the metrics.", file=sys.stderr)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
