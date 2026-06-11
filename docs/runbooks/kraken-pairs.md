# Kraken pair subscription

> **RETIRED 2026-06-10 (HOL-183).** The `kraken-ws-worker` container, `kraken_raw` dataset, pair manifest, and
> `ops:kraken:*` tasks were removed with the weather-only pivot. Kept as historical reference only.

Add, remove, list, or refresh the set of Kraken WS pairs the production `kraken-ws-worker` pool subscribes to. Companion runbook to [`series-subscription.md`](series-subscription.md); the two ops surfaces share shape but live in separate datasets and target separate workers.

## 1. Context

Desired subscription state lives in two places that must stay in sync. `kraken_raw.subscription_log` is the append-only event log and source of truth; every add/remove action lands here with a timestamp, actor, and reason. `kraken_raw.v_kraken_pairs_subscribed` is a derived view that collapses the log to the latest action per pair, returning only currently-subscribed pairs (`ORDER BY pair`).

`series-discovery` is the Cloud Run Job that reconciles the desired set into `gs://<your-archive-bucket>/control/kraken_pairs.json`, rewriting it atomically via `If-Generation-Match`. The same binary writes both `series_desired.json` AND `kraken_pairs.json` per run — they share REST/BQ/storage clients and the 6 h Cloud Scheduler trigger.

The desired Kraken pair set is the **union** of two sources:

1. **Catalog mirror.** `series-discovery` reads `kalshi_raw.v_series_subscribed`, runs each subscribed Kalshi ticker through `symbol.ExtractAssets` (substring-anchored match on `{BTC, DOGE, ETH, SOL, XRP}` against the `KX<asset>*` prefix), then maps each asset to its Kraken WS pair via `symbol.AssetToKrakenWSPair`. So a Kalshi-side `task ops:series:add` for `KX<crypto>D` auto-propagates to the Kraken side without operator action.
2. **Manual additions.** Pairs the operator subscribed via `task ops:kraken:add` land in `kraken_raw.subscription_log` and surface via `v_kraken_pairs_subscribed`. Use this for pairs that aren't yet reachable via a Kalshi-side subscription (Kraken-only pre-positioning, future asset).

`kraken-ws-worker` does two things with `kraken_pairs.json`:

- **Boot path.** Synchronous GCS load before the WS handshake (see `cmd/kraken-ws-worker/main.go`). On `ErrObjectNotExist`, the worker falls back to the `KRAKEN_PAIRS` env CSV (DR escape hatch); empty fallback → `os.Exit(1)`.
- **Steady state.** Reconciler goroutine polls the GCS object every 60 s ± 10 s jitter via `obj.Attrs().Generation` (skip-gate on unchanged generation). On generation bump, computes the diff vs the current in-process pair set and calls `Worker.AddPair` / `Worker.RemovePair` per delta entry.

> **Worker env vars.** `KRAKEN_PAIRS_OBJECT` selects the GCS object key (default `control/kraken_pairs.json`). `KRAKEN_PAIRS_BUCKET` selects the bucket; **when unset it inherits `KRAKEN_FLAG_BUCKET`** so the same bucket holds both `kraken_enabled.json` and `kraken_pairs.json` — the prod terraform leaves it unset on purpose. Set it explicitly only if you're splitting control objects across buckets (test envs, scratch).

End-to-end UX on operator add: `task ops:kraken:add` → `bq INSERT` (subscription_log) → `gcloud run jobs execute series-discovery --wait` (rewrites the manifest) → worker reconciler picks up within 60 s → AddPair runs per-pair AssetPairs preflight → WS subscribe → row flow within ~60 s of task exit.

## 2. Pre-conditions

### IAM

Operator must hold:

- `roles/bigquery.dataEditor` on dataset `kraken_raw`
- `roles/bigquery.jobUser` at project scope
- `roles/run.invoker` on the `series-discovery` Cloud Run Job

Project Owner satisfies all three for solo-dev.

### Kraken-side liveness

The `task ops:kraken:check` pre-flight probes `https://api.kraken.com/0/public/AssetPairs?pair=<urlencoded>` and PASSes iff the response `error` array is empty, the input pair appears as a top-level `result` key, and `status == "online"`. FAIL conditions:

- typo (`FOO/USD`) → `EQuery:Unknown asset pair`
- pair gated (e.g. `cancel_only`, `post_only`, `maintenance`) → status mismatch

The pre-flight talks to live Kraken, no auth needed. Counter cost 1.

## 3. Add a pair

**Step 1 — pick a Kraken pair** in the modern slash form. Hardcoded canonical 5: `BTC/USD`, `DOGE/USD`, `ETH/USD`, `SOL/USD`, `XRP/USD`. For non-canonical additions, confirm Kraken supports the pair via [the public AssetPairs catalogue](https://docs.kraken.com/api/docs/rest-api/get-tradable-asset-pairs).

> **Modern slash form vs `wsname`.** Always use the modern form (`BTC/USD`, `DOGE/USD`). `wsname` returns the legacy `XBT/USD` / `XDG/USD` for BTC and DOGE — WS v2 rejects those with `Currency pair not supported XBT/USD`. The pre-flight enforces the right shape via the asset-map indirection; if you're hand-crafting a pair, double-check.

**Step 2 — pre-flight.**

```sh
task ops:kraken:check PAIR=LTC/USD
```

A non-failing `PASS` is the only acceptable input to Step 3. Pass `FORCE=1` to `ops:kraken:add` if you want to record the audit row regardless (e.g. probing the per-pair AssetPairs fallback in the worker; documented opt-out).

**Step 3 — subscribe.**

```sh
task ops:kraken:add PAIR=LTC/USD REASON="why this pair"
```

`ops:kraken:add` invokes `ops:kraken:check` first; `FORCE=1` skips. On success, the task:

1. Inserts a `subscribe` row into `kraken_raw.subscription_log` via parameterized `bq query` (injection-safe).
2. Runs `gcloud run jobs execute series-discovery --wait` synchronously — `kraken_pairs.json` is rewritten before the task returns.
3. The worker's 60 s ± 10 s reconciler picks up the change and runs `Worker.AddPair`, which:
   - issues a per-pair AssetPairs preflight (with `AssetPairsWithFallback` bisect if a hypothetical batch lands a typo)
   - on success: `Bookkeeper.UpsertPairScale(pair, scale)` + WS subscribe `book` + WS subscribe `trade`
   - on rejection: logs a `subscription_log` row via the worker's BQ inserter (`action = "reject"`, `reason = verbatim Kraken error`)

Total wall-clock from task exit to rows landing in `kraken_raw.orderbook_snapshots_1s`: ≤60 s.

**Step 4 — verify.**

```sh
task ops:kraken:list

bq query --project_id=<your-gcp-project> --use_legacy_sql=false \
  "SELECT COUNT(*) AS rows, COUNT(DISTINCT envelope_id) AS distinct
   FROM \`<your-gcp-project>.kraken_raw.orderbook_snapshots_1s\`
   WHERE market_ticker = 'LTC/USD'
     AND event_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 3 MINUTE)"
```

`rows > 0 AND rows == distinct` is the binding check.

## 4. Remove a pair

```sh
task ops:kraken:remove PAIR=LTC/USD REASON="why this removal"
```

Same shape as `add` with `action='unsubscribe'`. Within ≤60 s the reconciler runs `Worker.RemovePair`, which sends WS unsubscribe + `Bookkeeper.DeleteSymbol(pair)` (frees per-symbol state + stops heartbeat emit). Rows stop arriving within ~90 s.

## 5. List

```sh
task ops:kraken:list
```

Returns the current `v_kraken_pairs_subscribed` content. Note this is the **operator-controlled** desired set; the catalog-mirror side may add pairs to `kraken_pairs.json` that don't appear here. To see the worker's actual subscription universe, `gcloud storage cat gs://<your-archive-bucket>/control/kraken_pairs.json`.

## 6. Recovery

### Per-pair AssetPairs rejection (operator typo)

Worker logs:

```
{"level":"WARN","msg":"kraken_addpair_rejected","pair":"FOOBAR/USD","kraken_err":"EQuery:Unknown asset pair"}
```

Rejection row appears in `kraken_raw.subscription_log` with `action='reject'`. Fix: `task ops:kraken:remove PAIR=FOOBAR/USD REASON="typo"` to clear from the desired set, then re-add with the correct slash form.

### WS subscribe ack `success: false` mid-session

Worker logs from HOL-48's dispatch handler (around `kraken_subscribe_ack`). Pair flipped to `cancel_only` / `maintenance` mid-session. Remove via `ops:kraken:remove` if the state is persistent.

### Boot-time control file absent

Worker logs `kraken_pairs_loaded bootstrap_source=env count=N` on a fresh env where `kraken_pairs.json` hasn't been written yet. DR fallback uses `KRAKEN_PAIRS` env CSV from `infra/terraform.tfvars`. Fix: run `task ops:kraken:discover` once to write the manifest, then either wait for the natural pool rollout or force one (`gcloud beta run worker-pools update kraken-ws-worker --region=us-east4 --update-env-vars=FORCE_RESTART=$(date +%s)`).

> **`bootstrap_source=env` outside a fresh-env window is a deploy-ordering bug, NOT normal DR.** The DR-fallback path silently picks up the static `KRAKEN_PAIRS` CSV and masks the GCS manifest entirely — operator adds via `ops:kraken:add` won't propagate on the next reconcile because there's no GCS object to diff against. If you see `bootstrap_source=env` on a steady-state pool, the tf rollout fired before `kraken_pairs.json` existed; re-order: seed `subscription_log` → `ops:kraken:discover` → THEN `tf apply` the worker-pool env change. Recovery: run `ops:kraken:discover` to write the manifest, then bump the pool revision (`gcloud beta run worker-pools update … --update-env-vars=FORCE_RESTART=$(date +%s)`) to re-exercise the boot path.

> **Kill-switch toggle is NOT a re-boot.** `task ops:kraken:on/off` re-enters the worker's outer loop but does NOT re-run `main.go`'s GCS pairs-load. Only a pool revision rollout (image bump OR env-var change) exercises the boot path.

### Concurrent scheduled + ops-triggered discovery

`If-Generation-Match` makes one writer the winner; the loser surfaces 412 in `series-discovery` logs and exits non-zero. Cloud Scheduler `retry_count=3` covers transient losses. No operator action required.

### `series-discovery` exit-code 2

Cloud Monitoring fires the same `series-discovery empty desired set` alert for both the Kalshi side (`empty desired set; refusing to write series_desired.json`) and the Kraken side (`empty kraken pair set; refusing to write kraken_pairs.json`). Disambiguate via slog before recovery:

```sh
gcloud logging read 'resource.type=cloud_run_job AND resource.labels.job_name=series-discovery AND severity=ERROR' \
  --project=<your-gcp-project> --limit=10 --format='value(jsonPayload.msg)'
```

- `empty desired set` → check `KALSHI_SERIES` tfvar / `task ops:series:list`; recovery via [`series-subscription.md`](series-subscription.md) §6.
- `empty kraken pair set` → check `kraken_raw.v_kraken_pairs_subscribed`; seed via `task ops:kraken:add` for the missing pair(s), then re-run discovery.

## 7. Discover (manual)

```sh
task ops:kraken:discover
```

Triggers a synchronous `series-discovery` Cloud Run Job execution. Use after a `series_desired.json` change if you want the matching `kraken_pairs.json` rewrite to happen immediately instead of waiting for the next 6 h tick.

## 8. References

- Design: internal design doc (not included in this public snapshot).
- Implementation plan: internal doc (not included in this public snapshot).
- Kraken-side skill: `.claude/skills/kraken-integration/SKILL.md`.
- Linear: [HOL-49](https://linear.app/holdlayer/issue/HOL-49) (parent [HOL-45](https://linear.app/holdlayer/issue/HOL-45)).
