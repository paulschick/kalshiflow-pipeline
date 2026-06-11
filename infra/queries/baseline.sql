-- 24h baseline numbers for kalshiflow. Used to populate the Baseline table in
-- docs/reference/system.md. Re-run after any change to ws-worker, parquet-exporter,
-- kalshi_series tfvar, or internal/metrics (per
-- ~/.claude/.../feedback_baseline_refresh_trigger.md).
--
-- Output: one row per (series_id, stream) plus three single-row "summary" sections
-- joined as a UNION ALL so a single query call answers everything.

WITH last_24h AS (
  SELECT TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR) AS lo,
         CURRENT_TIMESTAMP()                                  AS hi
),

snapshot_volume AS (
  SELECT 'snapshot_1s'                       AS stream,
         series_id,
         COUNT(*)                            AS row_count,
         AVG(LENGTH(raw_payload))            AS avg_bytes
  FROM `kalshi_raw.orderbook_snapshots_1s`, last_24h
  WHERE event_ts BETWEEN lo AND hi
  GROUP BY series_id
),
trade_volume AS (
  SELECT 'trade'                             AS stream,
         series_id,
         COUNT(*)                            AS row_count,
         AVG(LENGTH(raw_payload))            AS avg_bytes
  FROM `kalshi_raw.trade_events`, last_24h
  WHERE event_ts BETWEEN lo AND hi
  GROUP BY series_id
),
lifecycle_volume AS (
  SELECT 'lifecycle'                         AS stream,
         series_id,
         COUNT(*)                            AS row_count,
         AVG(LENGTH(raw_payload))            AS avg_bytes
  FROM `kalshi_raw.lifecycle_events`, last_24h
  WHERE event_ts BETWEEN lo AND hi
  GROUP BY series_id
),
settlement_volume AS (
  SELECT 'settlement'                        AS stream,
         series_id,
         COUNT(*)                            AS row_count,
         AVG(LENGTH(raw_payload))            AS avg_bytes
  FROM `kalshi_raw.settlement_events`, last_24h
  WHERE event_ts BETWEEN lo AND hi
  GROUP BY series_id
),
dlq_count AS (
  SELECT 'dlq' AS stream, '—' AS series_id, COUNT(*) AS row_count, NULL AS avg_bytes
  FROM `kalshi_raw.dead_letter_events`, last_24h
  WHERE publish_time BETWEEN lo AND hi
)

SELECT stream, series_id, row_count, avg_bytes
FROM snapshot_volume
UNION ALL SELECT * FROM trade_volume
UNION ALL SELECT * FROM lifecycle_volume
UNION ALL SELECT * FROM settlement_volume
UNION ALL SELECT * FROM dlq_count
ORDER BY stream, series_id;
