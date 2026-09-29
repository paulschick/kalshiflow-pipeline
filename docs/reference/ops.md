# kalshiflow — operations reference

All operational shortcuts live in `Taskfile.ops.yml` (included into the root `Taskfile.yml` under the `ops:` namespace).
Discover with `task -l | grep '^* ops:'`.

## Top reach-fors

| Task                                                  | What it does                                                                              |
|-------------------------------------------------------|-------------------------------------------------------------------------------------------|
| `task ops:worker:status`                              | Cloud Run worker pool — image, instance count, ready revision, condition                  |
| `task ops:worker:logs SINCE=5m`                       | Tail recent ws-worker logs                                                                |
| `task ops:worker:logs:errors SINCE=1h`                | Tail ws-worker logs at WARNING+                                                           |
| `task ops:bq:rows STREAM=snapshot_1s SINCE=10m`       | Row count + distinct envelope_id for a stream                                             |
| `task ops:bq:tail STREAM=snapshot_1s LIMIT=5`         | Tail most recent N rows for a stream                                                      |
| `task ops:bq:rows:by-series STREAM=snapshot_1s SERIES=KX... SINCE=10m` | Row count + distinct envelope_id for a (stream, series) pair              |
| `task ops:bq:dedup:check STREAM=snapshot_1s SINCE=1h` | Assert COUNT(*) == COUNT(DISTINCT envelope_id)                                            |
| `task ops:bq:cost:window SINCE=24h`                   | Per-stream rows + raw_payload bytes (cost re-baseline)                                    |
| `task ops:bq:row:decode-snapshot`                     | Fetch latest `orderbook_snapshots_1s` row, decode `raw_payload` via `cmd/decode-snapshot` |
| `task ops:series:add SERIES=KX... REASON="..."`       | Subscribe a Kalshi series (INSERT subscribe row in `subscription_log` + trigger `series-discovery`) |
| `task ops:series:remove SERIES=KX... REASON="..."`    | Unsubscribe a Kalshi series (INSERT unsubscribe row + trigger `series-discovery`)         |
| `task ops:series:list`                                | List currently-subscribed series (reads `v_series_subscribed`)                            |
| `task ops:series:discover`                            | Trigger `series-discovery` Cloud Run Job manually (refreshes `series_desired.json` + catalog tables) |
| `task ops:smoke:ws`                                   | Live WS smoke — T+0/+5/+15/+30 row-count ladder on `orderbook_snapshots_1s`               |
| `task ops:ps:topics`                                  | List `kalshi.*` topics with schema + encoding                                             |
| `task ops:ps:subs`                                    | List `kalshi.*.bq` subs with state + BQ schema flags                                      |
| `task ops:ps:schema:describe`                         | Describe Pub/Sub envelope schema (`kalshi-envelope-v1`)                                   |
| `task ops:ps:dlq:count SINCE=10m`                     | Count `dead_letter_events` rows in window                                                 |
| `task ops:tf:state:list`                              | `terraform state list`                                                                    |
| `task ops:tf:lock:show`                               | Read GCS-backend tflock object (who holds the apply lock)                                 |
| `task ops:image:current`                              | Print `ws_worker_image` from `infra/terraform.tfvars`                                     |
| `task ops:image:deploy TAG=vX.Y.Z`                    | Build + push `ws-worker:TAG` to Artifact Registry                                         |
| `task ops:dashboard:open`                             | Print Cloud Monitoring dashboard URL for kalshiflow                                       |
| `task ops:metrics:baseline`                           | Print 24h baseline numbers (rows/24h per stream×series + DLQ rows)                        |

The `task ops:kraken:*` surface (check/add/remove/list/discover, on/off/status) is retired — the Kraken side
(worker container, topics, `kraken_raw` tables, control objects) was removed 2026-06-10.

## Dashboard tiles worth knowing

`task ops:dashboard:open` opens the kalshiflow Cloud Monitoring dashboard. WS-stall + reconnect tiles in the WS source section:

- `ws_reconnects + watchdog force-reconnects rate` — TCP/WS-layer death. Fires when the existing pong-deadline watchdog (client-initiated ping every 30s, 15s pong deadline) detects an unresponsive WS protocol layer.
- `ws data-stall force-reconnects rate` — Kalshi app-layer freeze. Fires when no **trading** frame (`orderbook_snapshot`, `orderbook_delta`, `trade`) arrives for 90s (`KALSHI_WS_DATA_FRAME_DEADLINE`). Lifecycle frames do not refresh the deadline (per-channel split since `ws-worker:v0.13.0`).
- `ws_backoff_attempt_current` — exponential-backoff ladder position (Int64Gauge, recorded at every reconnect with the pre-reset attempt). 0 in steady state; > 0 during a sustained zero-trading-frame outage. Cap = `KALSHI_BACKOFF_MAX` (default 600s).
- `ws_connection_age` — current WS session uptime in seconds.

## Full list

```bash
task -l | grep '^* ops:'
```

Or read `Taskfile.ops.yml` directly — task definitions are the authoritative command shape.

## Cold tier (parquet-exporter)

List yesterday's exports per stream (substitute `YYYY/MM/DD` as needed):

```sh
gsutil ls gs://<your-archive-bucket>/snapshots_1s/year=2026/month=05/day=05/
gsutil ls gs://<your-archive-bucket>/trade/year=2026/month=05/day=05/
gsutil ls gs://<your-archive-bucket>/lifecycle/year=2026/month=05/day=05/
gsutil ls gs://<your-archive-bucket>/settlement/year=2026/month=05/day=05/
```

### Manual replay (parquet-exporter)

Re-run the exporter for a single historical date by setting `EXPORT_DATE` on
the Cloud Run job execution. Useful when a Cloud Scheduler run failed and the
retry window has lapsed.

```sh
gcloud run jobs execute parquet-exporter \
  --region=us-east4 \
  --update-env-vars=EXPORT_DATE=2026-05-01 \
  --wait
```

Replay covers all four Kalshi streams for the given date (the binary loops over
`streamConfigs()`; the two Kraken streams were removed 2026-06-10). Idempotent —
BQ Extract overwrites GCS objects in-place.
Cost is effectively zero (Extract is free; in-region GCS writes are free).

**No multi-day backfill recipe.** Pre-ship rows in trade/lifecycle/settlement
(≤14 days back) are NOT backfilled by design — forward-only coverage was the
explicit decision in the 2026-05-06 cold-tier coverage spec. Once each stream's
14-day BQ retention window passes, those pre-ship rows are gone. Reach for
`bq extract` directly per-stream if a one-off historical pull is ever worth
the manual work.

## Runbooks

| Runbook                                                              | When                                                                                                   |
|----------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------|
| [`series-subscription`](../runbooks/series-subscription.md)          | Add, remove, list, or refresh tracked Kalshi series (routine)                                          |
| [`update-kalshi-series`](../runbooks/update-kalshi-series.md)        | DR fallback only (both `series_desired.json` AND `subscription_log` gone) — deprecated for routine use |

## Caveats learned in production

- Do NOT alias BigQuery columns as `rows` in `bq query` — collides with the row-count column name.
- Do NOT combine `write_metadata=true` with `use_table_schema=true` on Pub/Sub→BQ subs — every row whose body schema
  doesn't match the table is silently rejected.
- Pre-protobuf rule: `raw_payload` JSON columns require `json.dumps()` before publish (BQ-direct subs hang silently
  otherwise). Since the 2026-05-04 protobuf cutover kalshiflow uses `BYTES` not JSON, so this no longer applies here —
  but the rule remains valid for projects that still use JSON columns.
