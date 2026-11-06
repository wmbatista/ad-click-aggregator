# Ad Click Aggregator

A hands-on implementation of the **Ad Click Aggregator** system design problem from
[Hello Interview](https://www.hellointerview.com/learn/system-design/problem-breakdowns/ad-click-aggregator).

The goal of this project is to go beyond the whiteboard: we will build, configure, and wire together
every component of the system, run it locally, exercise its use cases, and verify that it behaves as
designed.

> **Status:** early stage. The sections below describe the target state of the project; they will be
> filled in as each component is implemented.

## The Problem

An ad click aggregator collects click events from ads displayed across a platform and lets advertisers
query click metrics over time.

### Functional Requirements

1. **Click and redirect** — when a user clicks an ad, the click is recorded and the user is redirected
   to the advertiser's website.
2. **Query click metrics** — advertisers can query click metrics for their ads over time, with a
   minimum granularity of 1 minute.

### Non-Functional Requirements

- **Scalability** — support peak traffic of ~10k clicks per second.
- **Low-latency analytics** — metric queries should respond in under a second.
- **Fault tolerance and accuracy** — no click data should be lost; aggregated metrics must be accurate.
- **Near real-time** — metrics should be available for querying as soon as possible after a click.
- **Idempotency** — the same click must not be counted more than once.

## Tech Stack

- **Services:** [Kotlin](https://kotlinlang.org/) (JVM)
- **Messaging:** [Apache Kafka](https://kafka.apache.org/)
- **OLAP storage:** [ClickHouse](https://clickhouse.com/)
- **Local infrastructure:** Docker / Docker Compose

## Project Goals

By the end of this project we will have:

1. **A bootstrap script** that brings up every service and piece of infrastructure (Kafka, ClickHouse,
   the Kotlin services, etc.) with a single command.
2. **Use case simulation scripts** that trigger each functional requirement — e.g. generating ad clicks
   at configurable rates (including duplicate clicks) and querying metrics as an advertiser.
3. **Validation guides** that show how to verify each use case end to end — follow-up queries against
   ClickHouse, inspection of Kafka topics, service logs to check, and the expected results.

## Getting Started

_Coming soon._ This section will describe the prerequisites and how to run the bootstrap script.

## Repository Layout

_Coming soon._ This section will describe how services, infrastructure, scripts, and guides are
organized in the repository.

## References

- [Hello Interview — Design an Ad Click Aggregator](https://www.hellointerview.com/learn/system-design/problem-breakdowns/ad-click-aggregator)
