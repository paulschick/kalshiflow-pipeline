locals {
  # The orderbook delta tape is replaced by a derived 1s top-2 snapshot stream
  # in Plan 2.5. Kalshi-sent orderbook_snapshot frames are consumed by the
  # in-worker bookkeeper as book-reset markers and are NOT published.
  streams = ["snapshot_1s", "trade", "settlement", "lifecycle"]

  bq_table_for_stream = {
    snapshot_1s = "orderbook_snapshots_1s"
    trade       = "trade_events"
    settlement  = "settlement_events"
    lifecycle   = "lifecycle_events"
  }

  # Per-stream BQ partition expiration. snapshot_1s gets 7d (cold tier in Plan
  # 2.6 takes responsibility for long-term storage); other streams stay at the
  # original 14d.
  partition_expiration_ms_for_stream = {
    snapshot_1s = 604800000  # 7d
    trade       = 1209600000 # 14d
    settlement  = 1209600000
    lifecycle   = 1209600000
  }

  # Workload service accounts (created in slice 1.3b).
  # The `ksh-reconciler` SA was eliminated by the 2026-04-30 pivot — snapshot
  # anchors come from Plan 2.2's WS-frame classifier rerouting Kalshi-sent
  # `orderbook_snapshot` frames to `kalshi.snapshot`, plus Plan 2.4's reconnect
  # re-subscribes. No separate snapshot-publishing binary exists; the existing
  # publisher binding on `kalshi.snapshot` for `ksh-ws-worker` covers it.
  service_accounts = {
    ws_worker = "ksh-ws-worker"
    exporter  = "ksh-exporter"
  }

  archive_bucket = "${var.project_id}-kalshi-archive"
  ar_repo        = "kalshi"
}
