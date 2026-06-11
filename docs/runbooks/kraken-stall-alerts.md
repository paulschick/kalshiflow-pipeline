# Runbook — Kraken WS/bookkeeper stall alerts

> **RETIRED 2026-06-10 (HOL-183).** The alert policies and the `kraken-ws-worker` container were removed with
> the weather-only pivot. Kept as historical reference only.

Three Cloud Monitoring alert policies surface different failure modes that the
existing BQ row-floor alerts cannot. Filed under HOL-71, driven by the HOL-69
incident (2026-05-20 22:46Z → 2026-05-21 13:41Z: 14h 55m of OTel→Cloud
Monitoring rejection while BQ rows kept flowing on heartbeat).

## What each alert means

| Policy | Filter | Threshold | What it catches |
|---|---|---|---|
| `kraken_ws_book_input_silence` | `workload.googleapis.com/kraken_ws_messages_received_total`, `channel=book` | sum-rate < 10/sec in 5 min | Upstream Kraken WS book channel is producing no frames. The 1 Hz bookkeeper heartbeat masks this on the BQ-row alert; this policy keys on WS input grain. |
| `kraken_bookkeeper_change_silence` | `workload.googleapis.com/kraken_bookkeeper_snapshots_emitted_total`, `emit_reason=change` | sum-rate < 1/sec in 5 min | Bookkeeper-side stall — WS input may still be flowing, but the apply loop has stopped producing change-reason emits. Plausible causes: checksum-recovery wedge, registry mutex deadlock, internal panic absorbed by recover(). |
| `otel_points_out_of_order` | `logging.googleapis.com/user/otel_points_out_of_order`, derived from `jsonPayload.msg:"Points must be written in order"` on `resource.labels.worker_pool_name="ws-worker"` | delta > 0 in 5 min | OTel→Cloud Monitoring write rejection (HOL-69 fingerprint). When the SDK is in this state both metric-based alerts above go blind because the series produces no datapoints at all. |

### Reading combinations

| Firing set | Most likely cause |
|---|---|
| `kraken_ws_book_input_silence` only | Upstream Kraken WS book channel stopped. Trade channel may still be alive. Check `kraken_raw.trade_events` freshness to confirm the WS session itself is up. |
| `kraken_ws_book_input_silence` + `kraken_bookkeeper_change_silence` | Either both upstream and bookkeeper stalled (rare double fault), or upstream silence is propagating through bookkeeper. Treat as upstream first. |
| `kraken_bookkeeper_change_silence` only | Bookkeeper apply loop is stuck while WS frames are still arriving. Inspect `kraken_ws_silent_stall_force_reconnects_total` and recent ws-worker logs for `checksum mismatch`, `applying delta`, or panic-recover lines. |
| `otel_points_out_of_order` only | OTel-side resource poisoning. Metric-based alerts above are blind in this state — do not wait on them to clarify. Apply the OTel reset mitigation below. |
| `otel_points_out_of_order` + either metric alert | First mitigate the OTel rejection (the metric alerts may be false negatives caused by the rejection itself). Re-evaluate after the new revision is Ready. |

## Triage

All commands assume `project=<your-gcp-project>`, `region=us-east4`.

### Step 1 — Is the data pipeline actually affected?

```sh
bq query --project_id=<your-gcp-project> --use_legacy_sql=false \
  'SELECT "kraken_book" AS stream, MAX(event_ts) AS max_event,
          TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(event_ts), SECOND) AS age_s
   FROM `<your-gcp-project>.kraken_raw.orderbook_snapshots_1s`
   WHERE event_ts > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 MINUTE)
   UNION ALL
   SELECT "kraken_trade", MAX(event_ts),
          TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(event_ts), SECOND)
   FROM `<your-gcp-project>.kraken_raw.trade_events`
   WHERE event_ts > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 MINUTE)'
```

`age_s` near zero on both streams means BQ ingest is still flowing. That does
**not** prove the book channel is live — heartbeat alone keeps `age_s` low.
Continue to Step 2.

### Step 2 — Is the book channel actually producing deltas, or only heartbeat?

The definitive test: distinct payload count per pair per hour across the
suspect window. Heartbeat-only mode produces ~1 distinct payload per pair per
hour; a live book produces thousands.

```sh
bq query --project_id=<your-gcp-project> --use_legacy_sql=false \
  'SELECT market_ticker,
          TIMESTAMP_TRUNC(event_ts, HOUR) AS hr,
          COUNT(*) AS rows_,
          COUNT(DISTINCT TO_BASE64(raw_payload)) AS distinct_payloads
   FROM `<your-gcp-project>.kraken_raw.orderbook_snapshots_1s`
   WHERE event_ts BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 HOUR)
                      AND CURRENT_TIMESTAMP()
   GROUP BY market_ticker, hr
   ORDER BY market_ticker, hr'
```

Healthy steady-state distinct count is 2700–6400 per pair per hour at 5 pairs.
Counts near rows-per-hour ÷ 600 (i.e. ~5–10) indicate heartbeat-only mode.

Single-row decode for a tighter spot check:

```sh
TICKER='BTC/USD'
START=$(date -u -v-10M '+%Y-%m-%d %H:%M:%S')
END=$(date -u '+%Y-%m-%d %H:%M:%S')
bq query --project_id=<your-gcp-project> --use_legacy_sql=false --format=json --quiet \
  "SELECT TO_BASE64(raw_payload) AS b64, event_ts
   FROM \`<your-gcp-project>.kraken_raw.orderbook_snapshots_1s\`
   WHERE market_ticker='$TICKER'
     AND event_ts BETWEEN TIMESTAMP '$START' AND TIMESTAMP '$END'
   ORDER BY event_ts DESC LIMIT 10" \
  | jq -r '.[].b64' \
  | while read b64; do echo "---"; echo "$b64" | go run ./cmd/decode-snapshot; done
```

`emit_reason=change` on most/all rows with shifting `top1`/`top2` values
confirms a live book. All-heartbeat rows with frozen top-of-book values
confirm a real stall.

### Step 3 — Confirm or rule out OTel resource poisoning

```sh
gcloud logging read \
  'resource.type="cloud_run_worker_pool" \
   AND resource.labels.worker_pool_name="ws-worker" \
   AND jsonPayload.msg:"Points must be written in order"' \
  --project=<your-gcp-project> --freshness=2h --limit=20 \
  --format='value(timestamp, resource.labels.container_name, jsonPayload.msg)'
```

Any matches in the past 2h → OTel poisoning is active. Skip ahead to
mitigation.

### Step 4 — Worker pool state

```sh
gcloud run worker-pools describe ws-worker \
  --region=us-east4 --project=<your-gcp-project> \
  --format='value(status.latestReadyRevisionName, status.observedGeneration, metadata.generation)'
```

A revision rollover during a firing incident is fine — the alert filters do
not pin `node_id`. `metadata.generation > observedGeneration` indicates a
deploy in flight; wait for parity before further mitigation.

## Mitigation

### Mitigation A — OTel resource poisoning (alert `otel_points_out_of_order`)

The only known unwedge: force a new pool revision so the OTel SDK gets a
fresh `node_id` resource, which resets Cloud Monitoring's per-series
start_time history.

```sh
gcloud run worker-pools update ws-worker \
  --region=us-east4 --project=<your-gcp-project> \
  --update-labels=otel-reset=$(date +%s)
```

A label change is sufficient — no env-var or image bump needed. The new
revision is Ready within ~30 s; first successful OTel write follows within
60 s of Ready.

> **The `otel-reset` label persists on the pool indefinitely.** With the
> `hashicorp/google ~> 7.30` provider, keys not declared in the terraform
> `labels` block live in read-only `effective_labels` and are not
> reconciled out by `terraform apply` (verified empirically — see HOL-72
> dossier). The label is functionally inert outside the moment of
> application; its only purpose is to force a new revision. Clean up on a
> future tf-config-side touch of the pool, or on demand:
>
> ```sh
> gcloud run worker-pools update ws-worker \
>   --region=us-east4 --project=<your-gcp-project> \
>   --remove-labels=otel-reset
> ```
>
> **Persist in IaC: no (HOL-72 decision).** Under incident conditions the
> gcloud one-liner is faster than a `var.otel_reset` tfvar bump →
> `task ops:tf:apply` cycle. The recovery audit trail lives in this
> runbook + Linear (HOL-69 timeline + HOL-72 dossier), not in tf state.

Verify recovery:

```sh
# 1. New revision Ready
gcloud run worker-pools describe ws-worker --region=us-east4 --project=<your-gcp-project> \
  --format='value(status.latestReadyRevisionName, status.conditions[0].lastTransitionTime)'

# 2. No new rpc errors on the new revision
gcloud logging read \
  'resource.type="cloud_run_worker_pool" \
   AND resource.labels.worker_pool_name="ws-worker" \
   AND jsonPayload.msg:"Points must be written in order"' \
  --project=<your-gcp-project> --freshness=10m --limit=5

# 3. OTel metric datapoints landing on the new node_id. Post-HOL-111 each
#    container exports a distinct per-boot node_id of the form
#    "<K_REVISION>/<service>/<boot-id>" (e.g.
#    ws-worker-00041-bpx/kraken-ws-worker/8f6e33c4acb79e5a). The /<service>
#    segment (HOL-70) separates sibling containers; the /<boot-id> segment
#    (HOL-111, kraken from HOL-115 / v0.3.2) makes every process start
#    distinct so an in-place restart cannot collide on a shared start_time.
END=$(date -u '+%Y-%m-%dT%H:%M:%SZ')
START=$(date -u -v-5M '+%Y-%m-%dT%H:%M:%SZ')
curl -s -G -H "Authorization: Bearer $(gcloud auth print-access-token)" \
  --data-urlencode 'filter=metric.type="workload.googleapis.com/kraken_ws_messages_received_total" AND metric.labels.channel="book"' \
  --data-urlencode "interval.startTime=$START" \
  --data-urlencode "interval.endTime=$END" \
  --data-urlencode 'aggregation.alignmentPeriod=60s' \
  --data-urlencode 'aggregation.perSeriesAligner=ALIGN_RATE' \
  'https://monitoring.googleapis.com/v3/projects/<your-gcp-project>/timeSeries' \
  | jq -r '.timeSeries[] | "\(.resource.labels.node_id)\t\(.points[0].value.doubleValue // .points[0].value.int64Value)"'

# 4. BQ continuity across the cutover. A gap-free per-minute row count
#    across the restart timestamp confirms the bookkeeper kept publishing
#    real deltas to BQ even while OTel writes were rejected (HOL-69
#    Phase 8.4 verification pattern).
bq query --project_id=<your-gcp-project> --use_legacy_sql=false \
  'SELECT TIMESTAMP_TRUNC(event_ts, MINUTE) AS minute,
          COUNT(*) AS rows_
   FROM `<your-gcp-project>.kraken_raw.orderbook_snapshots_1s`
   WHERE event_ts > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 15 MINUTE)
   GROUP BY minute
   ORDER BY minute'
```

Empty log read in (2) + at least one row per (node_id) in (3) with a
non-zero rate + gap-free per-minute counts in (4) → mitigated.

### Mitigation B — Upstream Kraken WS book silence (alert `kraken_ws_book_input_silence` only)

This is usually Kraken-side instability that the worker cannot fix. The
silent-stall watchdog (HOL-54) should already be force-reconnecting; verify:

```sh
curl -G -H "Authorization: Bearer $(gcloud auth print-access-token)" \
  --data-urlencode 'filter=metric.type="workload.googleapis.com/kraken_ws_silent_stall_force_reconnects_total"' \
  --data-urlencode "interval.startTime=$(date -u -v-30M '+%Y-%m-%dT%H:%M:%SZ')" \
  --data-urlencode "interval.endTime=$(date -u '+%Y-%m-%dT%H:%M:%SZ')" \
  --data-urlencode 'aggregation.alignmentPeriod=300s' \
  --data-urlencode 'aggregation.perSeriesAligner=ALIGN_RATE' \
  'https://monitoring.googleapis.com/v3/projects/<your-gcp-project>/timeSeries' \
  | jq '.timeSeries[].points[].value.doubleValue // .timeSeries[].points[].value.int64Value'
```

If the watchdog is firing and reconnects are not restoring book frames, force
a revision rollover (same command as Mitigation A) to rebuild the pair state
from scratch. Otherwise watch upstream status at
<https://status.kraken.com/>.

### Mitigation C — Bookkeeper stall (alert `kraken_bookkeeper_change_silence` only)

Almost always a code-side fault. Tail the pool logs:

```sh
gcloud logging read \
  'resource.type="cloud_run_worker_pool" \
   AND resource.labels.container_name="kraken-ws-worker" \
   AND severity>=WARNING' \
  --project=<your-gcp-project> --freshness=15m --limit=50 \
  --format='value(timestamp, severity, jsonPayload.msg, textPayload)'
```

Look for checksum-mismatch loops, panic-recover lines, or apply-loop errors.
A revision rollover (Mitigation A) restarts the bookkeeper state machine; do
that as a temporary unwedge while filing a fix issue.

## Verification of the alerts themselves

After `task -y ops:tf:apply`:

```sh
# 1. New policies exist
gcloud alpha monitoring policies list --project=<your-gcp-project> \
  --format='value(displayName)' \
  | grep -E 'kraken_ws_messages_received_total|kraken_bookkeeper|Points must be written'

# 2. Log-derived metric exists
gcloud logging metrics describe otel_points_out_of_order --project=<your-gcp-project>

# 3. Smoke #3 end-to-end (writes a synthetic match into a test log; clean
#    up after the incident appears in the mobile app). Substitute a test
#    log name to avoid contaminating ws-worker telemetry.
gcloud logging write \
  --project=<your-gcp-project> \
  --severity=ERROR \
  --payload-type=text \
  hol-71-smoke 'Points must be written in order'
```

Note that smoke #3 will not actually fire the alert because its log filter
pins `resource.type="cloud_run_worker_pool"` and the `gcloud logging write`
synthetic log lands under `resource.type="global"`. The smoke confirms the
log metric is matchable on the substring; end-to-end fire validation
requires the real failure mode or a deliberate ws-worker pool log injection
(out of scope for this runbook — file an incident-rehearsal issue if needed).

## Incidents

### HOL-111 (2026-05-30→31): in-place-restart start_time poisoning

On 2026-05-30→31 an in-place ws-worker pool restart wedged every OTel
cumulative metric series for ~25h across both pool containers (`ws-worker`
Kalshi + `kraken-ws-worker` Kraken). BQ ingest ran normally throughout —
metrics-visibility outage only, no data loss.

The incident went undetected because the deployed `otel_points_out_of_order`
log-based metric filter matched **zero logs**: it specified
`resource.labels.service_name` (does not exist on worker-pool logs) and
`textPayload` (OTel SDK rejection logs land in `jsonPayload.msg`). This runbook
has been corrected to use `resource.labels.worker_pool_name` and
`jsonPayload.msg` throughout.

**Root cause:** an in-place restart (no new revision) leaves `K_REVISION`
unchanged, so the OTel SDK re-uses the same `host.id` / `node_id` resource.
Cloud Monitoring sees the restarted process attempt to write cumulative-counter
points with a new start_time that predates the last-known start_time for that
series and rejects them until ~25h have elapsed.

**Fix (this slice — HOL-111):** each process start now derives a per-boot
`node_id` of the form `<K_REVISION>/<service>/<boot-id>`. A restart inside the
same revision generates a per-boot token (8 random bytes from crypto/rand, hex-encoded — 16 hex chars), so the series is new to Cloud Monitoring
and OTel writes succeed immediately. The revision-rollover gcloud one-liner in
Mitigation A is no longer needed after an ordinary restart.

**Revision-rollover still needed** for any legacy-revision series already
poisoned before this fix shipped. If OTel rejection recurs on an un-upgraded
revision, self-heal is ~25h; the Mitigation A rollover shortens it to ~60s.

## References

- HOL-71 (this alert work).
- HOL-69 — Incident post-mortem; investigation log
  (internal design docs, not included in this public snapshot) and data-impact phase 10 doc on Linear.
- HOL-70 — Per-container OTel `node_id` split (the reason the alert filters
  do not pin `node_id`).
- HOL-72 — OTel start_time recovery runbook (this Mitigation A); decided
  against persisting `otel-reset` in IaC.
- HOL-54 — Kraken silent-stall watchdog.
- `infra/cloud-monitoring-alerts.tf` — alert + log-metric definitions.
- `infra/dashboards/main.json` — companion dashboard tiles for the three
  metrics.
