# kalshiflow — data paths

## In: Kalshi WebSocket channels

The `ws-worker` binary subscribes to three Kalshi WS channels (constants in `internal/kalshi/ws.go` and
`internal/worker/worker.go`):

- `orderbook_delta` — per-market orderbook deltas. NOT published directly; consumed by the in-worker bookkeeper (
  `internal/orderbook/`) which emits a derived 1-second top-2 BBO+1 snapshot stream.
- `trade` — trade/fill feed.
- `market_lifecycle_v2` — exchange-wide market lifecycle events.

Frame routing (`internal/worker/worker.go::classifyStream`):

| Kalshi WS frame       | event_type | Internal stream            |
|-----------------------|------------|----------------------------|
| `orderbook_delta`     | n/a        | bookkeeper → `snapshot_1s` |
| `trade`               | n/a        | `trade`                    |
| `market_lifecycle_v2` | `settled`  | `settlement`               |
| `market_lifecycle_v2` | other      | `lifecycle`                |

**Discovery** is in-worker via the `market_lifecycle_v2` channel + a periodic 3min ±30s REST sweep that closes the gap
where Kalshi does not emit `activated`/`created` lifecycle frames (originally observed on the KXBTCD/KXETHD hourly
markets, retired 2026-06-10). See `internal/worker/worker.go::sweepLoop`.

**Reconnect** is bookkeeper-driven; snapshot anchors come from a WS-frame classifier in `internal/worker/`. No separate
snapshot-reconciler job exists.

**Lifecycle eviction.** Terminal `market_lifecycle_v2` events (`event_type` ∈ {`settled`, `determined`, `deactivated`})
remove the market from the in-worker `Roster` and call `Bookkeeper.DeleteMarket(ticker)` (which deletes the entry from
the underlying `orderbook.Registry`). Default since `ws-worker:v0.12.0`: NO outbound `WS.Unsubscribe` is issued — Kalshi
auto-retires sids server-side at terminal state, and explicit unsubs caused a `code:7 Unknown subscription ID` flood at
rollover boundaries (2026-05-08 12:01 UTC outage). `KALSHI_LIFECYCLE_UNSUB=true` restores the legacy explicit-unsub
behavior. Sweep-loop trim (`Worker.sweepLoop` → `sweepOnce`) provides belt-and-suspenders eviction for markets that
disappeared without a terminal lifecycle event.

## Wire: protobuf envelope

- Schema source: `proto/kalshiflow/envelope/v1/envelope.proto`
- Snapshot payload: `proto/kalshiflow/orderbook/v1/snapshot.proto`
- Codegen: `protoc` + `protoc-gen-go` (NOT `buf`).
- Pub/Sub schema encoding: `BINARY` (see `infra/pubsub-topics.tf` and `infra/pubsub-schemas.tf`).
- BigQuery `raw_payload` column type: `BYTES`.
  - `snapshot_1s` rows: `proto.Marshal(SnapshotPayload)` bytes.
  - All other streams: UTF-8 JSON bytes.
- Heartbeat cadence: `internal/orderbook/bookkeeper.go::defaultHeartbeat` (currently 300s).

## Pub/Sub topics

Created via `for_each` over `local.streams` in `infra/pubsub-topics.tf` (streams listed in `infra/locals.tf`).

| Topic                | Stream                                                 | Notes                                          |
|----------------------|--------------------------------------------------------|------------------------------------------------|
| `kalshi.snapshot_1s` | derived 1-second top-2 BBO+1 orderbook snapshot        | publisher: bookkeeper                          |
| `kalshi.trade`       | trade feed                                             |                                                |
| `kalshi.lifecycle`   | non-settled `market_lifecycle_v2` events               |                                                |
| `kalshi.settlement`  | `market_lifecycle_v2` events with `event_type=settled` |                                                |
| `kalshi.deadletter`  | DLQ for failed Pub/Sub→BQ ingest                       | defined separately in `infra/pubsub-topics.tf` |
| `kalshi.orderbook`   | raw orderbook deltas                                   | REMOVED 2026-05-04 (replaced by snapshot_1s)   |
| `kalshi.snapshot`    | snapshot anchors                                       | REMOVED 2026-05-04 (reconciler eliminated)     |

Subscriptions (Pub/Sub → BQ): `infra/pubsub-bq-subscriptions.tf`. Pub/Sub schema definitions: `infra/pubsub-schemas.tf`.

## BigQuery (hot tier)

- Project: `kalshiflow` (or as set in `infra/terraform.tfvars`)
- Dataset: `kalshi_raw`
- Tables (mapping in `infra/locals.tf::bq_table_for_stream`, mirrored in `Taskfile.ops.yml::STREAM_TABLE_MAP`):

| Stream        | Table                    |
|---------------|--------------------------|
| `snapshot_1s` | `orderbook_snapshots_1s` |
| `trade`       | `trade_events`           |
| `lifecycle`   | `lifecycle_events`       |
| `settlement`  | `settlement_events`      |
| (DLQ)         | `dead_letter_events`     |

- Schema source-of-truth: `internal/envelope/schema.json`. Consumed by both Go (
  `internal/envelope/schema.go //go:embed`) and Terraform (`infra/bigquery.tf` via `file()`). Update both readers in
  lockstep if you restructure the repo layout.
- Table creation: `infra/bigquery.tf`.
- Partitioning: `DAY` on `event_ts`, clustering on `series_id`, `contract_id`. Per-stream `expiration_ms` in
  `infra/locals.tf::partition_expiration_ms_for_stream` — `snapshot_1s` is 7 days (cold tier owns longer retention),
  other streams 14 days.

## GCS cold tier

- Bucket: `${var.project_id}-kalshi-archive` (e.g. `<your-archive-bucket>`).
- Coverage: all four streams (forward-only since 2026-05-06). Per-stream Hive-partitioned, Snappy-compressed:

| Stream        | Path                                                   |
|---------------|--------------------------------------------------------|
| `snapshot_1s` | `snapshots_1s/year=YYYY/month=MM/day=DD/*.parquet`     |
| `trade`       | `trade/year=YYYY/month=MM/day=DD/*.parquet`            |
| `lifecycle`   | `lifecycle/year=YYYY/month=MM/day=DD/*.parquet`        |
| `settlement`  | `settlement/year=YYYY/month=MM/day=DD/*.parquet`       |

- Daily exporter: `cmd/parquet-exporter/`. Single Cloud Run job invocation per day; loops sequentially over
  `streamConfigs()` (snapshot_1s → trade → lifecycle → settlement). Idempotent overwrite — BQ Extract replays
  overwrite GCS objects in-place. `raw_payload` round-trips as Parquet `binary` (BYTE_ARRAY).
- Failure handling: all-or-nothing — first stream-level failure exits non-zero and Cloud Scheduler retries the
  whole job per its `retry_config`.
- Bucket configuration: `infra/gcs-archive.tf`.
- Cloud Run Job definition: `infra/cloud-run-jobs.tf`. Daily Cloud Scheduler trigger: `infra/cloud-scheduler.tf`.

### Retired: crypto/Kraken archive (LOCAL-ONLY)

The `kraken/snapshots_1s/` and `kraken/trade/` prefixes (and all crypto-series rows in the Kalshi prefixes,
2026-05-04 → 2026-06-10) are no longer collected and are being deleted from GCS under HOL-183. The surviving
copies are local-only:

- Primary: `~/.cache/kalshiflow-backtest/9056c8c/` (backtest repo mirror, pin `9056c8c`)
- Second copy: `/Users/paulschick/kalshiflow-archive-second-copy/9056c8c/`

Both include `bq_catalog/` Parquet exports of the no-expiry BQ tables (`series_catalog`, `market_catalog`,
`subscription_log`, `kraken_raw.subscription_log`). **The mirror root is keyed on the backtest repo's
`KALSHIFLOW_PIN_SHA_SHORT` — migrate the old pin dir on any pin bump or the data is orphaned.** Full details:
[`system.md`](system.md) "Weather-only pivot (HOL-183)".

## Control plane (series subscription)

- Source of truth: `kalshi_raw.subscription_log` (append-only event log; columns `ts`, `ticker`, `action`, `reason`, `actor`; defined in `infra/series-discovery.tf`).
- Derived view: `kalshi_raw.v_series_subscribed` (latest `action` per `ticker`, filtered to currently-subscribed only; orders by `ticker`).
- Reconciler: `series-discovery` Cloud Run Job. Triggers: 6 h Cloud Scheduler tick (production heartbeat) + on-demand via `task ops:series:{add,remove,discover}`. Writes `gs://<your-archive-bucket>/control/series_desired.json` atomically (`If-Generation-Match`).
- Worker contract: `ws-worker` polls the GCS object every 60 s via `obj.Attrs().Generation` (`internal/worker/desiredset.go`). On change, subscribes/unsubscribes per series in-place — no Cloud Run revision, no pod restart, in-process bookkeeper state for already-subscribed series is preserved.
- DR fallback: `KALSHI_SERIES` env (set via `infra/terraform.tfvars`) is read only if the GCS object is absent at worker boot (`cmd/ws-worker/main.go:355`). See [`docs/runbooks/series-subscription.md`](../runbooks/series-subscription.md) §9.
- Operator entry points: `task ops:series:{add,remove,list,discover}`. Audit-log queries: see `docs/runbooks/series-subscription.md` §7.
- **Kraken control plane — RETIRED 2026-06-10 (HOL-183).** The HOL-47 kill-switch (`control/kraken_enabled.json`, flipped off 2026-06-10T11:19:10Z) and the HOL-49 pair-set manifest (`control/kraken_pairs.json`, `kraken_raw.subscription_log` + `v_kraken_pairs_subscribed`) are gone along with the `kraken-ws-worker` container and all Kraken terraform. Runbooks [`kraken-flag.md`](../runbooks/kraken-flag.md) and [`kraken-pairs.md`](../runbooks/kraken-pairs.md) are kept as historical reference only.

## Service accounts

Defined in `infra/service-accounts.tf`, IAM bindings in `infra/iam-bindings.tf`:

- `ksh-ws-worker` — used by ws-worker Cloud Run worker pool (publishes to `kalshi.*` topics).
- `ksh-exporter` — used by parquet-exporter Cloud Run Job (reads BQ, writes GCS).
- Plus the scheduler invoker SA (separate, in `cloud-scheduler.tf`).
