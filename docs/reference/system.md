# kalshiflow — system reference

kalshiflow is a managed-services GCP pipeline that subscribes to Kalshi market WebSocket feeds
and persists every event to BigQuery (hot tier) and GCS Parquet (cold tier) for offline backtesting and ML research.
**Collection is weather-only as of 2026-06-10.** Active series = 16 weather tickers: KXHIGHNY, KXHIGHLAX,
KXHIGHCHI, KXHIGHMIA, KXHIGHDEN, KXHIGHTDAL, KXHIGHTPHX, KXHIGHTBOS, KXHIGHTSEA, KXHIGHPHIL, KXHIGHTDC, KXHIGHTHOU,
KXHIGHAUS, KXHIGHTSFO, KXHIGHTMIN, KXRAINNYC. The 1 s snapshot-on-change envelope contract is unchanged. The crypto
series and the entire Kraken spot side are retired — see "Weather-only pivot" below. Solo developer; the
single-container `ws-worker` pool (1 vCPU + 512 MiB always-on — the gen2 always-allocated floor) is the fixed-cost
dominator; everything else sits under free-tier thresholds at this scale. This doc is a thin pointer index — every claim links to a
source-of-truth file or has a verify-with-this-task recipe.

## Weather-only pivot (2026-06-10)

Cutover timeline (UTC):

| Event                          | Timestamp                                            | Evidence                                            |
|--------------------------------|------------------------------------------------------|------------------------------------------------------|
| Weather collection start       | 2026-06-10T00:32:10.145Z                              | `series_desired.json` generation (16 weather series added) |
| Crypto Kalshi cutoff           | 2026-06-10T11:18:41.842Z (worker pickup ≤ 11:19:42Z) | weather-only `series_desired.json` generation        |
| Kraken collection disabled     | 2026-06-10T11:19:10.000Z                              | `kraken_enabled.json` generation                     |

Retired crypto series (6): KXBTCD, KXDOGE, KXDOGED, KXETHD, KXSOLD, KXXRPD. Lifecycle `determined` events for
crypto markets already listed at cutoff may trail in for ~1 week post-cutoff — expected, not a leak.

**Historical crypto/Kraken data** (all 6 stream prefixes — Kalshi snapshots_1s/trade/lifecycle/settlement +
kraken/snapshots_1s, kraken/trade; 2026-05-04 → 2026-06-10 — plus Parquet exports of the no-expiry BQ tables
`series_catalog`, `market_catalog`, `subscription_log`, `kraken_raw.subscription_log`) was exported to offline storage
before the GCP-side deletion.

## Binaries

| Binary                          | Purpose                                                                   | Source                          | Image / how-to-run                                                                                         |
|---------------------------------|---------------------------------------------------------------------------|---------------------------------|------------------------------------------------------------------------------------------------------------|
| `ws-worker`                     | Subscribes Kalshi WS, publishes envelopes to Pub/Sub                      | `cmd/ws-worker/`                | image pinned in `infra/terraform.tfvars` (`ws_worker_image` var); deployed image: `task ops:worker:status` |
| `parquet-exporter`              | Daily BQ → GCS Parquet export for all four streams, idempotent overwrite  | `cmd/parquet-exporter/`         | Cloud Run Job in `infra/cloud-run-jobs.tf`; daily trigger in `infra/cloud-scheduler.tf`                    |
| `series-discovery`              | 6h Kalshi `/series` + `/markets` refresh; MERGEs hash-diffed rows into `kalshi_raw.series_catalog` and `kalshi_raw.market_catalog` | `cmd/series-discovery/`         | Cloud Run Job + Scheduler + alert in `infra/series-discovery.tf` (per-feature layout); image `series_discovery_image` in `infra/terraform.tfvars`. SA `ksh-series-discovery`. Added 2026-05-10. Markets pass added 2026-05-10. Operator surface: `subscription_log` (event log), `v_series_subscribed` (view), `control/series_desired.json` (GCS), `task ops:series:{add,remove,list,discover}`. |
| `decode-snapshot`               | Dev tool — decode protobuf `raw_payload` (BYTES) → JSON for human reading | `cmd/decode-snapshot/`          | run locally via `go run ./cmd/decode-snapshot` (also driven by `task ops:bq:row:decode-snapshot`)          |
| `scripts/dump_kalshi_series.py` | Export available Kalshi series via REST                                   | `scripts/dump_kalshi_series.py` | run locally (superseded for catalog purposes by `series-discovery` job + `kalshi_raw.series_catalog` table) |

## Infrastructure

- Terraform root: `infra/`
- One resource family per `<family>.tf`: `infra/cloud-run-worker-pool.tf` (ws-worker), `infra/cloud-run-jobs.tf` (
  parquet-exporter), `infra/cloud-scheduler.tf`, `infra/bigquery.tf`, `infra/pubsub-topics.tf`,
  `infra/pubsub-bq-subscriptions.tf`, `infra/pubsub-schemas.tf`, `infra/gcs-archive.tf`, `infra/iam-bindings.tf`,
  `infra/service-accounts.tf`, `infra/secret-manager.tf`, etc.
- Image-pinning convention: `infra/cloud-run-worker-pool.tf` reads from the `ws_worker_image` variable, set in
  `infra/terraform.tfvars`. Bumped via `terraform apply`.
- Workload-identity SA bindings: `infra/iam-bindings.tf`. Service-account definitions: `infra/service-accounts.tf`.
- Apply: `task tf:apply`. Targeted apply: `task ops:tf:apply:target TARGET=<resource>`.

## Observability

- Dashboard: `kalshiflow` Cloud Monitoring dashboard. Open via `task ops:dashboard:open`.
- Provisioned in `infra/cloud-monitoring-dashboard.tf` (JSON: `infra/dashboards/main.json`).
- Sections (data-flow order): WS source → Pub/Sub publish → Pub/Sub→BQ ingest → BQ tables → Cold tier → Worker health.
- Custom metrics defined in `internal/metrics/metrics.go`; `ws-worker` emits volume metrics, `parquet-exporter` and `series-discovery` emit run metrics (the latter under `series_discovery_run_outcome_total` / `_run_duration_seconds` / `_rows_inserted_total{kind=catalog|market}`). Custom metrics surface under `workload.googleapis.com/<name>` with resource type `generic_node`.
- WS-stall liveness — two independent watchdogs:
    - **TCP/WS layer** (pong-deadline): client-initiated ping every 30s with 15s pong deadline; force-reconnect on miss. Emits `ws_watchdog_force_reconnects_total`.
    - **Kalshi app layer** (data-stall watchdog, `Worker.dataStallLoop`): tracks **trading-frame** liveness only. Inbound frames are bucketed: trading (`orderbook_snapshot`, `orderbook_delta`, `trade`) refresh the deadline; lifecycle (`market_lifecycle_v2` and unknown types; settlement events ride the same channel and are distinguished by payload) do not. This split closes the 2026-05-08 under-fire bug where exchange-wide lifecycle chatter at rollover boundaries masked per-market trading silence. Default deadline: 90s (`KALSHI_WS_DATA_FRAME_DEADLINE`). Tick interval: 5s. Force-reconnect emits `ws_data_stall_force_reconnects_total{trigger=data_timeout}`. The original two-trigger design was reduced to data-only on 2026-05-06 after empirical evidence showed Kalshi suppresses server pings under steady-state worker load.
    - **Per-tick heartbeat**: `ws_data_stall_ticks_total{result=ok|tripped|skipped}` increments on every `dataStallLoop` tick. Alert on `rate(...{result="ok"}) == 0` over 5min — proves the loop is alive independent of the force-reconnect counter.
    - **Panic-recover**: `ws_data_stall_panics_total` counts recovered panics inside the loop body. Should always be 0 in steady state; non-zero indicates a code bug forced the watchdog into the recover path (which still calls `WS.Close` to surface the failure as a normal reconnect).
- **Backoff ladder gauge**: `ws_backoff_attempt_current` (Int64Gauge, no labels) is recorded at every reconnect with the pre-reset attempt value. 0 = healthy reset (last session received ≥ `KALSHI_BACKOFF_DATA_THRESHOLD` trading frames before disconnect); > 0 = ladder escalating during a sustained zero-trading-frame outage. Used together with `ws_data_stall_force_reconnects_total` to distinguish a single flap from a sustained Kalshi-side stall.
- Alerts: P0 wired (DLQ row growth, parquet-exporter run failure, series-discovery run failure). WS-side and row-count regression alerts were deferred to later observability work.

## Worker tunables (env kill-switches)

All wired in `cmd/ws-worker/main.go` and passed through `Deps`. Pod-restartable via env without rebuild — flip the env on the Cloud Run worker pool revision, redeploy.

| Env                              | Type     | Default | Restore-legacy value | What it gates                                                                                                          |
|----------------------------------|----------|---------|----------------------|------------------------------------------------------------------------------------------------------------------------|
| `KALSHI_WS_WATCHDOG_ENABLED`     | bool     | `true`  | `false`              | Pong-deadline watchdog (TCP/WS layer)                                                                                  |
| `KALSHI_WS_WATCHDOG_PING_INTERVAL` | duration | `30s`   | —                    | Pong-deadline ping cadence                                                                                             |
| `KALSHI_WS_WATCHDOG_PONG_DEADLINE` | duration | `15s`   | —                    | Pong-deadline pong wait                                                                                                |
| `KALSHI_WS_DATA_STALL_ENABLED`   | bool     | `true`  | `false`              | Data-stall watchdog (Kalshi app layer)                                                                                 |
| `KALSHI_WS_DATA_FRAME_DEADLINE`  | duration | `90s`   | —                    | Data-stall trading-frame deadline                                                                                      |
| `KALSHI_PRESERVE_BOOKKEEPER`     | bool     | `true`  | `false`              | Lift `Bookkeeper.Run` to Worker lifetime (preserves heartbeat trickle across reconnects, Slice 1 / 2026-05-07)         |
| `KALSHI_BACKOFF_DATA_THRESHOLD`  | int     | `10`    | `0`                  | Reconnect-backoff reset gate. `0` restores legacy `dur > stableThreshold` (60s) reset. `>0` requires N trading frames. |
| `KALSHI_BACKOFF_MAX`             | duration | `600s`  | `60s`                | Cap of the exponential-backoff ladder                                                                                  |
| `KALSHI_LIFECYCLE_UNSUB`         | bool     | `false` | `true`               | Issue explicit `WS.Unsubscribe` on settled/determined/deactivated. `false` (default) avoids `code:7` flood at rollover boundaries; eviction relies on Kalshi's server-side auto-retire + Bookkeeper Registry delete. |

## Cost

### Volumes — measured 2026-05-07

Via `task ops:bq:cost:window SINCE=24h` at 4 series (KXBTCD, KXETHD, KXSOLD, KXXRPD):

| Stream        | Rows / 24h | Avg bytes | Total bytes / 24h |
|---------------|-----------:|----------:|------------------:|
| `snapshot_1s` |  1,317,719 |        24 |           31.6 MB |
| `trade`       |    149,569 |       228 |           34.2 MB |
| `lifecycle`   |     16,981 |       656 |           11.1 MB |
| `settlement`  |     16,267 |        93 |            1.5 MB |

Stored `raw_payload` ≈ 78 MB/day → ~2.4 GB/mo extrapolated.

### Cost breakdown — calculated 2026-05-17

Calculated via the Cloud Billing Catalog API (`gcp-cost` MCP) against the Kalshi 4–5-series volumes above + the Kraken 5-pair
volumes in the Kraken baseline subsection. Catalog SKU IDs in parens; verify with `https://cloud.google.com/run/pricing`
etc. The pool is a multi-container pod hosting `ws-worker` (Kalshi, `cpu=500m memory=256Mi`) and `kraken-ws-worker` (Kraken,
`cpu=500m memory=256Mi`) — pod sum = 1 vCPU + 512 MiB always-on. Supersedes the 2026-05-07 entry (pre-Kraken single-container)
and a briefly deployed 2 vCPU + 1 GiB shape that doubled cost vs plan until it was shrunk back to 1 vCPU + 512 MiB on 2026-05-17.

| Service                                                  | Monthly  | Notes                                                                                            |
|----------------------------------------------------------|---------:|--------------------------------------------------------------------------------------------------|
| Cloud Run worker pool (1 vCPU + 512 MiB always-on; multi-container before the weather-only pivot, single-container after — same shape, same cost) | $16.84 | SKUs `77E7-D935-BA67` (CPU $15.97) + `967D-19B3-F10F` (mem $0.88), us-east4, 730.5h/mo, post-free-tier; re-verified via gcp-cost MCP 2026-06-10 |
| Cloud Run job (parquet-exporter ~10 min/day)                          | ~$0.00 | Free-tier remainder absorbs it after ws-worker                                                    |
| Cloud Run job (series-discovery 6h cron, ~30s/run)                    | ~$0.00 | Free-tier remainder                                                                              |
| GCS Standard (~2.5 GiB recent + Kraken archive growth)               | ~$0.06 | SKU `5F7A-5173-CF5B`; older data tiers via `infra/gcs-archive.tf` lifecycle                       |
| BigQuery active logical storage (~1 GiB Kalshi + Kraken)             | $0.00  | Under 10 GiB-month free tier                                                                     |
| BigQuery query (on-demand)                                            | $0.00  | <1 TiB/mo scanned; under free tier                                                               |
| Pub/Sub Message Delivery Basic (Kalshi + Kraken ~3 GiB/mo)            | $0.00  | Under 10 GiB-month free tier; SKU `FCD2-1531-9A6F`                                               |
| Cloud Monitoring metric volume (~80 MiB est. post-Kraken)             | $0.00  | Under 150 MiB-month free tier (verify if cardinality grows)                                      |
| Cloud Logging                                                         | ~$0    | Assumed under 50 GiB-month free tier (not verified)                                              |
| Artifact Registry image storage (~1.5 GiB)                            | ~$0.15 | $0.10/GiB-month                                                                                  |
| **Total**                                                             | **~$17** |                                                                                                |

Worker pool dominates and is fixed-cost (independent of series/pair count at the current shape) — adding the 6th–10th Kalshi
series and 6th–10th Kraken pair will add to volumes but those volumes stay well within free tiers, so the marginal cost is
near-zero until either (a) Pub/Sub crosses 10 GiB/mo, (b) Cloud Monitoring custom-metric volume crosses 150 MiB/mo (more
time-series cardinality), or (c) BQ active storage crosses 10 GiB/mo. The Kalshi container is sized for the 10–20-series
target; if soak testing shows it cliffs before 20 series at `cpu=500m`, scale the container limit up rather than
the instance count. Verify current burn against billing console; `task ops:bq:cost:window SINCE=24h` re-checks the volume
side of the input.

### Cost delta — weather-only pivot (2026-06-10)

- **Worker pool compute: $0 delta — NOT the ~$8.42/mo the pre-pivot research projected.** Removing the
  `kraken-ws-worker` sidecar does NOT shed its nominal 0.5 vCPU / 256 MiB, because the pool was already at the
  gen2 always-allocated floor (total CPU ≥ 1 vCPU, total memory ≥ 512 MiB). The old 2-container pod sat exactly
  at that floor (2× 500m/256Mi = 1 vCPU + 512 MiB); the surviving single `ws-worker` container is pinned to
  1 vCPU / 512 MiB to satisfy the same floor — identical billed shape, identical $16.84/mo (re-verified via
  gcp-cost MCP 2026-06-10: CPU `77E7-D935-BA67` $15.97 + mem `967D-19B3-F10F` $0.88). A sub-floor pool would
  require the gen1 (throttled) execution environment, unsuitable for an always-on WS collector.
- **Real savings are data-volume + storage, not compute:** crypto Kalshi (~1.24M snap rows/day for KXBTCD alone,
  the dominant row-rate) + all Kraken streams (~167k snap + ~213k trade rows/day) no longer ingested — cutting
  Pub/Sub Message Delivery, BQ streaming inserts, and BQ/GCS storage growth. The standing GCS crypto/Kraken
  archive (~5.3 GiB, SKU `5F7A-5173-CF5B`/`D5C1-AAF1-5B8F`) was deleted (~$0.07–0.11/mo rent removed and no
  longer growing); the `kraken_raw` BQ dataset was dropped. These are all within free tiers at current scale, so
  the realized monthly-bill decrease is small in absolute dollars — the win is halting unbounded growth of a
  no-consumer dataset, not a line-item cut.
- Export egress for the archive copy-out: $0 (free tier) + ≤$0.05 one-time Nearline retrieval (measured).

## Cold tier

The `parquet-exporter` Cloud Run job runs daily at 01:00 UTC and writes the
previous UTC day's BQ partition for each stream as Snappy-compressed Parquet
under `gs://<your-archive-bucket>/<prefix>/year=YYYY/month=MM/day=DD/*.parquet`.
Coverage is exhaustive across all four streams as of 2026-05-06:

| Stream        | BQ table                  | GCS prefix      | BQ retention |
|---------------|---------------------------|-----------------|--------------|
| snapshot_1s   | orderbook_snapshots_1s    | snapshots_1s    | 7 days       |
| trade         | trade_events              | trade           | 14 days      |
| lifecycle     | lifecycle_events          | lifecycle       | 14 days      |
| settlement    | settlement_events         | settlement      | 14 days      |

Failure handling is all-or-nothing: the first stream-level failure exits the
job non-zero and Cloud Scheduler retries the whole run (BQ Extract is
idempotent — replays overwrite GCS objects in-place).

## Baseline

> **Superseded by the weather-only pivot (2026-06-10).** The tables below are the last crypto-era
> baseline, kept for the historical record. The project was torn down (2026-06-11) before a post-pivot baseline at the
> 16-weather-series set was taken.

Kalshi side measured 2026-05-11 over the prior 24h via `task ops:metrics:baseline` at 5 series (KXBTCD, KXDOGED, KXETHD, KXSOLD, KXXRPD) on `ws-worker:v0.15.0` + `parquet-exporter:v0.3.0`. Supersedes the 2026-05-08 baseline. Kraken side: pair set is now dynamic (2026-05-15) — union of catalog mirror over `v_series_subscribed` + operator additions in `kraken_raw.subscription_log`; per-(stream, pair) volumes are in the Kraken subsection below.

| Stream                         | Series                | Rows / 24h | Avg bytes |
|--------------------------------|-----------------------|-----------:|----------:|
| snapshot_1s                    | KXBTCD                |  1,032,251 |        24 |
| snapshot_1s                    | KXDOGED               |    110,374 |        23 |
| snapshot_1s                    | KXETHD                |    303,728 |        24 |
| snapshot_1s                    | KXSOLD                |    317,465 |        23 |
| snapshot_1s                    | KXXRPD                |     94,788 |        24 |
| trade                          | KXBTCD                |    218,192 |       229 |
| trade                          | KXDOGED               |        376 |       230 |
| trade                          | KXETHD                |      7,060 |       227 |
| trade                          | KXSOLD                |      1,763 |       227 |
| trade                          | KXXRPD                |        237 |       226 |
| lifecycle                      | KXBTCD                |      8,808 |       590 |
| lifecycle                      | KXDOGED               |      2,751 |       555 |
| lifecycle                      | KXETHD                |      3,530 |       593 |
| lifecycle                      | KXSOLD                |      3,600 |       572 |
| lifecycle                      | KXXRPD                |      3,530 |       673 |
| settlement                     | KXBTCD                |     14,956 |        93 |
| settlement                     | KXDOGED               |      1,379 |        95 |
| settlement                     | KXETHD                |      1,765 |        92 |
| settlement                     | KXSOLD                |      1,800 |        92 |
| settlement                     | KXXRPD                |      1,765 |        91 |
| dlq                            | —                     |          0 |         — |
| ws_watchdog_force_reconnects   | —                     |          0 |         — |
| ws_data_stall_force_reconnects | trigger=data_timeout  |          1 |         — |
| ws_backoff_attempt_current     | —                     |          1 | (max obs) |

5-series steady state on `ws-worker:v0.15.0` — KXDOGED was added during the slice-3 smoke 2026-05-10 and is now visible across all four streams. Volumes scale linearly with series count; the per-series-per-stream rates for KXBTCD, KXETHD, KXSOLD, KXXRPD are consistent with the 2026-05-08 reading once normalized for the longer measurement window.

WS-stall health is quiet: `ws_data_stall_force_reconnects_total` fires once in the 24h window (down from 3 on 2026-05-08), `ws_watchdog_force_reconnects_total` is 0 (pong-deadline watchdog had nothing to fire on), and `ws_backoff_attempt_current` maxes at 1 — a transient single-step ladder bump with immediate reset to 0, consistent with a single short Kalshi-side blip that received the data-threshold count of frames before reconnect.

Refresh: `task ops:metrics:baseline` for the per-(stream, series) numbers; query `ws_watchdog_force_reconnects_total`, `ws_data_stall_force_reconnects_total`, and `ws_backoff_attempt_current` over a 24h window via Cloud Monitoring REST for the counter/gauge rows. Supersede in place — git owns history.

### Kraken — RETIRED 2026-06-10; shape below (2026-05-15)

> The Kraken side is fully retired: `kraken_enabled.json` flipped off 2026-06-10T11:19:10Z, container removed
> from the pool, topics/subs/`kraken_raw` tables/flag deleted via terraform. Historical data was exported
> offline (see "Weather-only pivot" above). The subsection below describes the retired system.

`kraken-ws-worker` collected WS v2 `book` (depth=10) + `trade` for a dynamic pair set computed from two sources: the catalog mirror over `kalshi_raw.v_series_subscribed` (Kalshi-tracked assets auto-propagate to Kraken via the `KX<asset>*` prefix extractor in `internal/kraken/symbol.ExtractAssets`) plus operator-controlled additions in `kraken_raw.subscription_log`. Bookkeeper-aggregated 1 s change-tick + 300 s heartbeat per `(pair, side)`; trade frames passthrough one envelope per trade. Gated by a GCS-backed kill switch (`control/kraken_enabled.json`).

| Resource                                       | Shape                                                                       |
|------------------------------------------------|-----------------------------------------------------------------------------|
| Worker pool                                    | Shares `ws-worker` pool (us-east4, multi-container pod totaling 1 vCPU + 512 MiB, manual instance count 1) — runs as sidecar container `kraken-ws-worker` with `cpu=500m memory=256Mi` after the 2026-05-17 shrink (consolidated into one pool 2026-05-15). |
| Topics                                         | `kraken.snapshot_1s`, `kraken.trade` (`kalshi-envelope-v1` schema, BINARY)  |
| BQ tables                                      | `kraken_raw.orderbook_snapshots_1s` (7 d), `kraken_raw.trade_events` (14 d) |
| BQ control                                     | `kraken_raw.subscription_log` (audit, retain forever) + `kraken_raw.v_kraken_pairs_subscribed` (latest-action-per-pair view) |
| Pair set source                                | union of catalog mirror (over `kalshi_raw.v_series_subscribed`) + `v_kraken_pairs_subscribed`; written to `control/kraken_pairs.json` |
| Operator surface                               | `task ops:kraken:{check,add,remove,list,discover}` — mirrors `ops:series:*` shape |
| DLQ                                            | reuses `kalshi.deadletter` topic + sub                                      |
| Cost (verified via `gcp-cost` MCP, 2026-05-17) | ~$0.04 / mo incremental (shares `ws-worker` pool with kalshi; only Pub/Sub→BQ delivery is Kraken-attributable). Pool cost ~$16.84 / mo single-pool (1 vCPU + 512 MiB pod, 500m/256Mi per container). SKUs `77E7-D935-BA67` + `967D-19B3-F10F` + `FCD2-1531-9A6F`. Supersedes the 2026-05-15 entry which claimed `~$16.83 / mo unchanged`; in reality the deployed shape (1000m/512Mi per container = 2 vCPU + 1 GiB pod) cost ~$33.68 / mo until the 2026-05-17 shrink corrected it. |

Per-(stream, pair) row volumes measured 2026-05-14 22:44Z → 2026-05-15 22:43Z on `kraken-ws-worker:v0.2.0`, 5 stable pairs.

| Stream      | Pair     | Rows / 24h | Avg bytes |
|-------------|----------|-----------:|----------:|
| snapshot_1s | BTC/USD  |     64,611 |        30 |
| snapshot_1s | DOGE/USD |     66,913 |        35 |
| snapshot_1s | ETH/USD  |     67,127 |        32 |
| snapshot_1s | SOL/USD  |     62,695 |        31 |
| snapshot_1s | XRP/USD  |     65,928 |        34 |
| trade       | BTC/USD  |     33,882 |       149 |
| trade       | DOGE/USD |     12,296 |       153 |
| trade       | ETH/USD  |     16,280 |       148 |
| trade       | SOL/USD  |     16,731 |       146 |
| trade       | XRP/USD  |     19,064 |       149 |

WS health (24h sums, Cloud Monitoring REST, `ALIGN_DELTA`):

| Counter                                | 24h sum     |
|----------------------------------------|------------:|
| `kraken_ws_messages_received_total`    |  7,404,839  |
| `kraken_ws_reconnects_total`           |        133  |
| `kraken_ws_silent_stall_force_reconnects_total` |        133  |
| `kraken_ws_subscribe_rejected_total`   |        125  |
| `kraken_ws_checksum_mismatches_total`  |  5,523,796  |
| `kraken_ws_per_symbol_resubscribes_total` |     11,307  |
| `kraken_ws_resub_dropped_total`        |  5,512,489  |
| `kraken_bookkeeper_snapshots_emitted_total` |    330,002  |
| `kraken_pubsub_publish_failures_total` |          0  |
| `kraken_pubsub_drain_unflushed_total`  |          0  |

24h dedup-clean confirmed: 328,165 snapshot rows / 328,165 unique envelope_id; 98,381 trade rows / 98,381 unique envelope_id. The reconnect-storm symptom is contained (133/24h, all silent-stall-driven; no subscribe-rate trips) but the underlying checksum-mismatch rate of ~64/s was never root-caused before Kraken was retired. The dropped-resub counter equals the mismatch counter 1:1, proving the rate-limiter is doing its containing job.

### Control plane

| Object                                                           | Source of truth                                                                          | Writer                              | Reader                                       |
|------------------------------------------------------------------|------------------------------------------------------------------------------------------|-------------------------------------|----------------------------------------------|
| `control/series_desired.json`                                     | `kalshi_raw.v_series_subscribed`                                                         | `series-discovery` (6 h cron + adhoc) | `ws-worker` (60 s poll, `Attrs.Generation`)  |
| `control/kraken_pairs.json`                              | RETIRED 2026-06-10 — no longer written or read                                  | —                                   | —                                            |
| `control/kraken_enabled.json`                            | RETIRED 2026-06-10 — flipped off 11:19:10Z, then infra deleted                  | —                                   | —                                            |

The `series_desired.json` write uses an atomic `If-Generation-Match` pattern (`internal/seriescat.WriteDesiredSet`) so concurrent writers (scheduled 6 h tick + operator-triggered run) cannot lose updates — the loser surfaces 412 and Cloud Scheduler `retry_count=3` covers transient losses.

## Verifying live state

| Question                                  | Command                                               |
|-------------------------------------------|-------------------------------------------------------|
| What image is running?                    | `task ops:worker:status`                              |
| Are rows landing?                         | `task ops:bq:rows STREAM=snapshot_1s SINCE=10m`       |
| Are envelope_ids unique?                  | `task ops:bq:dedup:check STREAM=snapshot_1s SINCE=1h` |
| What's the latest snapshot row look like? | `task ops:bq:row:decode-snapshot`                     |
| Is the WS worker actually publishing?     | `task ops:smoke:ws` (T+0/+5/+15/+30 row-count ladder) |
| Cost since N?                             | `task ops:bq:cost:window SINCE=24h`                   |
| How is the pipeline performing?           | `task ops:dashboard:open`                             |

Full task list: `task -l`.
