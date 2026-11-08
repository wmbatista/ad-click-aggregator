-- Shared contract between stream-processor (writer) and query-service (reader).
-- See docs/superpowers/specs/2026-11-08-shared-contracts-design.md.
--
-- Writers only INSERT delta rows; SummingMergeTree adds rows with the same (ad_id, minute) on merge.
-- Merges are asynchronous, so readers must always use sum(click_count) with GROUP BY.
CREATE DATABASE IF NOT EXISTS ad_clicks;

CREATE TABLE IF NOT EXISTS ad_clicks.clicks_per_minute
(
    ad_id       String,
    minute      DateTime('UTC'),   -- start of the 1-minute bucket
    click_count UInt64
)
ENGINE = SummingMergeTree(click_count)
PARTITION BY toYYYYMMDD(minute)
ORDER BY (ad_id, minute);
