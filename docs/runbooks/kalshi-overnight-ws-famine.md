# Runbook — Kalshi overnight WS data-stall famine

Recurring, expected behavior: during the 08–10Z UTC low-volume window
(≈ 4–6 AM ET), the Kalshi WS session frequently goes minutes-to-tens-of-
minutes without a single trading-bucket frame (`orderbook_snapshot`,
`orderbook_delta`, `trade`) while lifecycle/settlement heartbeats keep
the underlying TCP alive. The Kalshi ws-worker's data-stall watchdog
(`internal/worker/worker.go:1375` `dataStallLoop`, 90 s
`dataFrameDeadline` at `worker.go:206`/`worker.go:392`) correctly trips
on each gap. It will cycle (force-close → exp-backoff reconnect → REST
enumerate + initial subscribe → 90 s grace → next trip) until upstream
volume returns near US pre-open (≈ 09:00–09:10Z).

Filed under HOL-73. Driven by
the 5/21 incident inside the larger
HOL-69 window; data-impact
phase 10 of HOL-69 confirmed
zero data loss
during the watchdog-cycling sub-window.

## Why this is expected, not a regression

The watchdog cares about `lastTradingDataAt` (`worker.go:791`), which is
refreshed only on `bucketTrading` frames per the dispatcher at
`worker.go:788–801`. Lifecycle and settlement frames refresh
`lastLifecycleAt` separately and do **not** reset the trip deadline. This
split was added in the 2026-05-08 under-fire fix so exchange-wide
lifecycle chatter cannot mask per-market trading silence at series
rollover boundaries.

During the overnight UTC window, Kalshi keeps streaming lifecycle/
settlement heartbeats but produces no order-book deltas or trades on the
configured series for stretches well past the 90 s deadline. From the
watchdog's perspective this is indistinguishable from a silent TCP
half-stall, so it force-reconnects on schedule. The new session also
finds the upstream quiet, so it trips again 90 s after its grace
expires. Reconnecting does not fix it because nothing on the worker
side is broken — there is simply no trading data to be delivered.

## Fingerprint

All four signals must match. Two or three matching with one missing
points to a different failure mode (see "When to escalate" below).

1. **Window.** 08–10Z UTC (97 % of 30-day `data_timeout` reconnects
   cluster here; per HOL-73 — Gap analysis: WS trading-frame famine on 5/07, 5/14, 5/21).
2. **Trigger.** `workload.googleapis.com/ws_data_stall_force_reconnects_total{trigger="data_timeout"}`
   firing in a tight cycle (typically 1m 36s → 4m 1s between trips).
   `trigger="ping_timeout"` is a different code path (`pingLoop`,
   `worker.go` pong-deadline watchdog) and is **not** this runbook.
3. **Trading-frame rate.** `workload.googleapis.com/ws_messages_received_total{service_name="ws-worker"}`,
   summed at 5-min `ALIGN_RATE`, split by `stream`: `orderbook_delta` +
   `trade` literally at zero for one or more 5-min windows;
   `lifecycle` continues at its baseline ≈ 0.6–0.7 msg/s,
   `settlement` ≈ 0.04 msg/s.
4. **Container shape.** Pool revision Ready=True, instance count 1.0,
   memory flat (~7 % was observed on 5/21), no `panic` log lines, no
   `ws data-stall loop panic`.

The combined signature is the watchdog working as designed against real
upstream silence — not a local detection bug, not a TCP half-stall, not
a goroutine wedge.

## Triage

### Step 1 — Confirm the window + trigger

```sh
TOKEN=$(gcloud auth print-access-token)
START=$(date -u -v-2H +"%Y-%m-%dT%H:%M:%SZ")
END=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
curl -s -G -H "Authorization: Bearer $TOKEN" \
  --data-urlencode 'filter=metric.type="workload.googleapis.com/ws_data_stall_force_reconnects_total"' \
  --data-urlencode "interval.startTime=$START" \
  --data-urlencode "interval.endTime=$END" \
  --data-urlencode 'aggregation.alignmentPeriod=300s' \
  --data-urlencode 'aggregation.perSeriesAligner=ALIGN_DELTA' \
  --data-urlencode 'aggregation.groupByFields=metric.label.trigger' \
  'https://monitoring.googleapis.com/v3/projects/<your-gcp-project>/timeSeries' \
  | jq -r '.timeSeries[] | "\(.metric.labels.trigger // "?")\t\([.points[].value.int64Value // .points[].value.doubleValue // 0] | add)"'
```

`trigger=data_timeout` with a non-zero count during 08–10Z → continue.
Outside that band, or `trigger=ping_timeout` → not this runbook.

### Step 2 — Confirm trading-frame famine, not local detection bug

```sh
DAY=$(date -u +%Y-%m-%d)
curl -s -G -H "Authorization: Bearer $TOKEN" \
  --data-urlencode 'filter=metric.type="workload.googleapis.com/ws_messages_received_total" AND metric.labels.service_name="ws-worker"' \
  --data-urlencode "interval.startTime=${DAY}T06:00:00Z" \
  --data-urlencode "interval.endTime=${DAY}T11:00:00Z" \
  --data-urlencode 'aggregation.alignmentPeriod=300s' \
  --data-urlencode 'aggregation.perSeriesAligner=ALIGN_RATE' \
  --data-urlencode 'aggregation.crossSeriesReducer=REDUCE_SUM' \
  --data-urlencode 'aggregation.groupByFields=metric.label.stream' \
  'https://monitoring.googleapis.com/v3/projects/<your-gcp-project>/timeSeries'
```

Expected during famine:

- `stream=orderbook_delta` and `stream=trade` collapse to ≈ 0 msg/s for
  one or more 5-min buckets (a 0.000 5-min rate means literally no frame
  arrived in 300 s — far over the 90 s deadline).
- `stream=lifecycle` stays at its baseline (≈ 0.6–0.7 msg/s for the
  current series mix); `stream=settlement` ≈ 0.04 msg/s. These confirm
  the TCP is alive and Kalshi is still emitting non-trading frames.

If `lifecycle` and `settlement` are also at zero, the failure is not
this one (TCP itself is silent) — investigate as a connectivity stall.

### Step 3 — Confirm BQ continuity (data-impact check)

```sh
bq query --project_id=<your-gcp-project> --use_legacy_sql=false \
  'SELECT TIMESTAMP_TRUNC(event_ts, MINUTE) AS m, COUNT(*) AS rows_
   FROM `<your-gcp-project>.kalshi_raw.orderbook_snapshots_1s`
   WHERE event_ts > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 HOUR)
   GROUP BY m ORDER BY m'
```

A flat per-minute row count equal to the active-ticker bookkeeper
heartbeat rate during the famine is the expected shape — heartbeat
snapshots keep flowing even when no inbound trading frames arrive
(bookkeeper `Run` is process-lifetime per
`Deps.PreserveBookkeeper=true` at `worker.go:213-219`). A drop to zero
rows is a different failure mode (bookkeeper stall) and out of scope
for this runbook.

## Mitigation

**None during the window.** The cycling is the watchdog catching real
upstream silence; force-reconnecting again from the operator side does
not help. Wait for upstream volume to return at ≈ 09:00–09:10Z; the next
batch of `dynamic subscribe` log lines stabilizes the session
automatically (5/21 example: mass `dynamic subscribe` at 09:08:06Z
ended a 68 min reconnect-cycle stretch).

If the window has clearly closed (10:30Z+ ET, US pre-open in progress)
and `data_timeout` reconnects are still firing, treat that as a
different fault — see "When to escalate."

## When to escalate

Any of:

- `data_timeout` reconnects continue past 10:30Z UTC (US pre-open is
  well underway; upstream silence at that point is unexpected).
- `data_timeout` reconnects fire **outside** the 08–10Z window — that
  is a new failure mode, not this one. The current outlier worth keeping
  separate is 2026-05-06 14–15Z, which fired as `ping_timeout`, not
  `data_timeout`; do not bundle them.
- `stream=lifecycle` rate also drops to zero in the per-stream split
  above (Step 2) — TCP itself is silent; treat as a connectivity stall
  and force a revision rollover to rebuild the session:

  ```sh
  gcloud run worker-pools update ws-worker \
    --region=us-east4 --project=<your-gcp-project> \
    --update-labels=session-reset=$(date +%s)
  ```

  Same caveat as Mitigation A in
  [`kraken-stall-alerts.md`](kraken-stall-alerts.md): the label
  persists on the pool indefinitely via `effective_labels` (HOL-72
  decision: not persisted in IaC).
- BQ `kalshi_raw.orderbook_snapshots_1s` per-minute row count drops to
  zero (not just trading-frame famine + heartbeat continuity) — the
  bookkeeper itself has stalled, which is a separate fault.

## References

- HOL-73 — investigation
  + decision tree this runbook closes.
- HOL-73 — Pre-investigation data: 30-day pattern + verified code refs
  — 30-day histogram, code-reference validation, recommended queries.
- HOL-69 — parent incident
  context. Phase 10 verification doc confirms zero data loss during the
  5/21 watchdog-cycling sub-window.
- `internal/worker/worker.go:1350-1434` — `dataStallLoop` (the spec
  comment block above the function documents the
  trading-vs-lifecycle-bucket split).
- `internal/worker/worker.go:788-801` — read-loop dispatcher
  (where `lastTradingDataAt` / `lastLifecycleAt` are refreshed).
- `internal/worker/worker.go:702-708` — session-start grace
  initialization + `ws session config` log line.
- Silent WS stall fingerprint (heartbeat-only BQ rows), HOL-54
  — separate failure mode where the TCP is alive but bookkeeper
  heartbeat is the only thing flowing; this runbook is the opposite
  shape (trading silence + lifecycle heartbeat still flowing).
