# Series subscription

Add, remove, list, or refresh the set of Kalshi series the production `ws-worker` pool subscribes to. This is the routine-use runbook; the historical `update-kalshi-series` runbook is retained only for DR (see §9).

## 1. Context

Desired subscription state lives in two places that must stay in sync. `kalshi_raw.subscription_log` is the append-only event log and source of truth; every add/remove action lands here with a timestamp, actor, and reason. `kalshi_raw.v_series_subscribed` is a derived view that collapses the log to the latest action per ticker, returning only currently-subscribed series.

`series-discovery` is the Cloud Run Job that reconciles the view into `gs://<your-archive-bucket>/control/series_desired.json`, rewriting it atomically via `If-Generation-Match` to prevent lost updates from concurrent writers. It runs on a 6 h scheduled trigger and is also invokable on demand.

`ws-worker` polls the GCS object every 60 s using `obj.Attrs().Generation` (see `internal/worker/desiredset.go`) to detect changes without re-downloading the body when nothing changed. The `KALSHI_SERIES` env var in `infra/terraform.tfvars` serves as a DR-only fallback at boot when the GCS object is absent (see `cmd/ws-worker/main.go:355`).

## 2. Pre-conditions

### IAM

Operator must hold:

- `roles/bigquery.dataEditor` on dataset `kalshi_raw`
- `roles/bigquery.jobUser` at project scope
- `roles/run.invoker` on the `series-discovery` Cloud Run Job

Project Owner satisfies all three for solo-dev.

### `SESSION_USER()` actor semantics

The `actor` column in `subscription_log` is populated from `SESSION_USER()` at insert time. Three cases:

- developer terminal → gcloud-authenticated principal (e.g. `paul@paul-schick.com`) — "a human ran this"
- CR Job / CI runner → runtime SA email (e.g. `ksh-series-discovery@<your-gcp-project>.iam.gserviceaccount.com`) — "automation ran this"
- `gcloud config set auth/impersonate_service_account SA_EMAIL` → impersonated SA, not the human (acceptable, still pins the IAM principal that authorized the action) — worth knowing when reading the log.

## 3. Add a series

**Step 1 — pick a ticker.**

```sh
bq query --use_legacy_sql=false \
  "SELECT ticker, title, frequency FROM \`<your-gcp-project>.kalshi_raw.series_catalog\`
   WHERE ticker LIKE 'KX%' AND category = 'Climate and Weather'
   ORDER BY ticker LIMIT 50"
```

This replaces the old `scripts/dump_kalshi_series.py` step — `series_catalog` is the in-cluster mirror, maintained by the 6 h discovery sweep.

`frequency=daily` series may be between rollover windows; rows won't flow until the next daily contract opens. Prefer `frequency=hourly` for fast smoke verification.

**Step 2 — pre-flight: confirm the candidate has live markets on Kalshi.**

`series_catalog` mirrors Kalshi's `/series` endpoint, which is **registration-only** — a row there means Kalshi declared the series, not that any contracts ever opened. Market liveness lives at `/markets`. The Kalshi web UI hides registration-only series, so they're invisible from the browser but present in our catalog. Subscribing to such a "phantom" series produces zero data silently — no DLQ row, no worker error, just empty BQ partitions until someone notices.

Refuse to subscribe unless the candidate has at least one open market on Kalshi:

```sh
task ops:series:check SERIES=KXFOO
```

Behind the task (copy-pasteable for ad-hoc probing):

```sh
curl -sS "https://api.elections.kalshi.com/trade-api/v2/markets?series_ticker=KXFOO&status=open&limit=1" \
  | jq '.markets | length'
```

A non-zero result is the only acceptable input to Step 3. If the check returns `0`, do not proceed. Documented opt-outs: pre-subscribing ahead of a scheduled market open, or exercising the subscribe-now-emit-later path during testing. In those cases, set `FORCE=1` on `task ops:series:add` (Step 3) and record the reason in `REASON=`.

Daily caveat: `frequency=daily` series open contracts at most once per day. The `/markets?status=open` probe is a point-in-time snapshot; if a daily series is between rollover windows, the check returns `0` and is a legitimate `FORCE=1` candidate provided you can name the expected open window in `REASON=`. Hourly series do not need this carve-out.

**Step 3 — subscribe.**

```sh
task ops:series:add SERIES=KXFOO REASON="why this series"
```

`ops:series:add` invokes `ops:series:check` first and aborts before any `subscription_log` insert if the pre-flight fails. `FORCE=1` skips the pre-flight (see Step 2 daily caveat for legitimate uses):

```sh
task ops:series:add SERIES=KXFOO REASON="pre-subscribe ahead of UTC-00 daily open" FORCE=1
```

Wall-clock ~10–30 s (BQ INSERT + synchronous CR Job).

**Step 4 — verify rows arrive within ~60 s (worker poll cadence).**

```sh
sleep 60 && task ops:bq:rows:by-series STREAM=snapshot_1s SERIES=KXFOO SINCE=2m
```

Pass condition: `row_count > 0`. If 0 and the series is `frequency=daily`, wait for the next rollover.

## 4. Remove a series

```sh
task ops:series:remove SERIES=KXFOO REASON="why no longer needed"
```

Worker stops within 60 s; BQ rows age out per partition `expiration_ms` (snapshot_1s 7 d; trade/lifecycle/settlement 14 d); no manual partition drop is part of this runbook.

## 5. List

```sh
task ops:series:list
```

Prints currently-subscribed ticker set, ordered by `ticker` (view-side).

## 6. Manual discovery refresh

```sh
task ops:series:discover
```

Reach for this when: suspected `series_desired.json` ↔ `v_series_subscribed` drift; stale `series_catalog` / `market_catalog`; on-demand catalog refresh ahead of the scheduled 6 h tick.

## 7. Audit log queries

Last 30 d of events:

```sql
SELECT ts, ticker, action, reason, actor
FROM `<your-gcp-project>.kalshi_raw.subscription_log`
WHERE ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
ORDER BY ts DESC;
```

One-ticker history:

```sql
SELECT ts, action, reason, actor
FROM `<your-gcp-project>.kalshi_raw.subscription_log`
WHERE ticker = 'KXBTCD'
ORDER BY ts DESC;
```

Who did what when (group by actor):

```sql
SELECT actor, action, COUNT(*) AS n, MAX(ts) AS last_seen
FROM `<your-gcp-project>.kalshi_raw.subscription_log`
WHERE ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
GROUP BY actor, action
ORDER BY n DESC;
```

Expect mixed human + SA rows in `actor`. Cross-reference §2 for interpretation.

## 8. Troubleshooting

- **"New series 0 rows after 5 min"** → check `worker_desired_set_stale` gauge in the kalshiflow Cloud Monitoring dashboard; cross-check daily-cadence rollover.
- **"Discovery exits 2 (`errEmptyDesiredSet`)"** → `v_series_subscribed` is empty. Query `kalshi_raw.subscription_log` for an unintended-all-unsubscribed sequence; re-add at least one ticker.
- **"Concurrent operator + scheduled discovery"** → safe by design (`If-Generation-Match` on the GCS write). Worst case: one extra CR Job execution (~$0 marginal), one retry log line. No manual intervention.
- **"Kalshi-side outage"** → cross-check with `task ops:smoke:ws`.

## 9. Bootstrap / DR fallback

1. Edit `infra/terraform.tfvars` to set `kalshi_series = "KXHIGHNY,..."` (CSV; the current desired set is the 16 weather series, weather-only since 2026-06-10).
2. `task tf:apply` — rolls a worker revision that boots from the env fallback.
3. Re-populate `kalshi_raw.subscription_log` with subscribe rows for each ticker:

   ```sh
   task ops:series:add SERIES=KXHIGHNY REASON="DR cold-start re-population"
   # repeat per ticker
   ```

4. `task ops:series:discover` — rewrites `series_desired.json` from the now-populated `v_series_subscribed`.
5. Worker picks up the GCS object on the next 60 s poll; the `KALSHI_SERIES` env path becomes inert until the next pod boot.

`docs/runbooks/update-kalshi-series.md` retains a stub of this section for back-compat link survival.

## 10. Cost

MCP-verified $/mo + SKU IDs for the slice-4 control-plane surface only. Full pipeline cost is in [`docs/reference/system.md`](../reference/system.md).

| SKU | Resource | Measured input | $/mo |
|---|---|---|---|
| `C709-E057-D6F9` | Cloud Run Job CPU (us-east4) | 125 runs/mo × ~150 s × 1 vCPU = 18,750 vCPU-s | $0.18/mo |
| `822B-0EF3-BCEF` | Cloud Run Job Memory (us-east4) | 125 runs/mo × ~150 s × 0.5 GiB = 9,375 GiB-s | $0.01/mo |
| `7870-010B-2763` | GCS Class B ops (worker `obj.Attrs()` poll) | 60 s ticks × 1 instance × 30 d = 43,200 ops | $0.00/mo (within tier) |
| `A5D6-43A2-28A7` | Cloud Scheduler (6 h discovery + DLQ alert) | 2 jobs/mo | $0.00/mo (3-job free tier) |

Slice-4 marginal control-plane cost: **~$0.19/mo**. Worker pool fixed-cost (~$15/mo, see [`docs/reference/system.md`](../reference/system.md)) continues to dominate. Inputs measured 2026-05-11: `run.googleapis.com/job/completed_execution_count` shows 17 runs since the 2026-05-10 series-discovery ship; scheduled cadence is 4/day → ~120/mo + ~5 operator triggers, wall-clock mean 151 s across the 17 observed runs (median ~130 s). GCS op rate derived from worker poll cadence × `ws_worker_instance_count = 1` — re-baseline if the worker pool scales out. Cloud Run free tier (per billing account) is consumed by the always-on `ws-worker` pool, so discovery runs are billed at full rate.
