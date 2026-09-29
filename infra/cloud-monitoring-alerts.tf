# Cloud Monitoring alert policies for kalshiflow. P0 signals only — DLQ row
# growth and parquet-exporter run failure. Each policy uses an empty
# notification_channels list; incidents surface to the user via the Cloud
# Console mobile app (per-user, per-device setting outside Terraform).
#
# Deferred signals (WS stall, data-stall loop dead, BQ row-count regression)
# were tracked as later observability work.

resource "google_monitoring_alert_policy" "dlq_growth" {
  display_name          = "kalshiflow / DLQ row growth"
  combiner              = "OR"
  notification_channels = []
  severity              = "WARNING"

  conditions {
    display_name = "dead_letter_events rows >= 1 in 5 min"

    condition_threshold {
      filter          = "resource.type = \"pubsub_subscription\" AND metric.type = \"pubsub.googleapis.com/subscription/dead_letter_message_count\""
      comparison      = "COMPARISON_GT"
      threshold_value = 0
      duration        = "0s"

      aggregations {
        alignment_period   = "300s"
        per_series_aligner = "ALIGN_DELTA"
      }
    }
  }

  alert_strategy {
    auto_close = "1800s"
  }
}

resource "google_monitoring_alert_policy" "parquet_exporter_failure" {
  display_name          = "kalshiflow / parquet-exporter run failed"
  combiner              = "OR"
  notification_channels = []
  severity              = "ERROR"

  conditions {
    display_name = "Cloud Run job execution failed"

    condition_threshold {
      filter = join(" AND ", [
        "resource.type = \"cloud_run_job\"",
        "resource.labels.job_name = \"parquet-exporter\"",
        "metric.type = \"run.googleapis.com/job/completed_execution_count\"",
        "metric.labels.result = \"failed\"",
      ])
      comparison      = "COMPARISON_GT"
      threshold_value = 0
      duration        = "0s"

      aggregations {
        alignment_period   = "300s"
        per_series_aligner = "ALIGN_DELTA"
      }
    }
  }

  alert_strategy {
    auto_close = "1800s"
  }
}

# HOL-52: the 2026-05-14 outage produced zero rows for 1h47m with no page.
# This alert fires on the first aligned 5-min window in which the snapshot
# writer drops below floor.
#
# Threshold rationale: healthy traffic is 400/min change-tick + 614+/min on
# heartbeat windows. 50 rows / 5 min covers the lowest realistic ETH-only
# weekend window (~50-100/min) without false-firing. NOTE: this floor is
# sized for the 2026-05 series count (≤ 4 series); the steady-state row
# rate scales ~linearly with series count, so a real outage at the 10–20
# series prod target (see MEMORY project_target_series_count) could drop
# from ~500–1000/min to ~200/min and still pass this floor. Revisit when
# KALSHI_SERIES is bumped past ~6.
#
# Timing: duration=0s + alignment_period=300s = page on the first aligned
# window that breaches (≈ 5 min after the outage starts). Do NOT raise
# duration unless flapping is observed — the PR's stated SLO is "page
# within 5 min", anything longer reintroduces the silent-outage gap the
# alert exists to close.
resource "google_monitoring_alert_policy" "bq_orderbook_snapshots_floor" {
  display_name          = "kalshiflow / orderbook_snapshots_1s row floor breached"
  combiner              = "OR"
  notification_channels = []
  severity              = "ERROR"

  conditions {
    display_name = "uploaded_row_count < 50 in 5 min on orderbook_snapshots_1s"

    condition_threshold {
      filter = join(" AND ", [
        "resource.type = \"bigquery_dataset\"",
        "resource.labels.dataset_id = \"kalshi_raw\"",
        "metric.type = \"bigquery.googleapis.com/storage/uploaded_row_count\"",
        "metric.labels.table = \"orderbook_snapshots_1s\"",
      ])
      comparison      = "COMPARISON_LT"
      threshold_value = 50
      duration        = "0s"

      aggregations {
        alignment_period   = "300s"
        per_series_aligner = "ALIGN_DELTA"
      }
    }
  }

  alert_strategy {
    auto_close = "1800s"
  }
}

# HOL-71: OTel start_time rejection log-derived metric. The HOL-69 failure
# mode rejects OTel writes with "Points must be written in order" and starves
# the affected series entirely — metric-channel silence alerts see no
# datapoints in that state, and a condition_threshold against a non-existent
# series will not reliably trip. The log channel survives an OTel resource
# poisoning and is the only signal that catches it in time to mitigate via
# revision rollover.
#
# First google_logging_metric in the repo; declared inline next to its alert
# policy for single-file ownership.
resource "google_logging_metric" "otel_points_out_of_order" {
  name        = "otel_points_out_of_order"
  description = "Count of ws-worker pool log lines reporting OTel CreateTimeSeries rejection with 'Points must be written in order' (HOL-69 fingerprint)."

  filter = join(" AND ", [
    "resource.type=\"cloud_run_worker_pool\"",
    "resource.labels.worker_pool_name=\"ws-worker\"",
    "jsonPayload.msg:\"Points must be written in order\"",
  ])

  metric_descriptor {
    metric_kind = "DELTA"
    value_type  = "INT64"
    unit        = "1"
  }
}

resource "google_monitoring_alert_policy" "otel_points_out_of_order" {
  display_name          = "kalshiflow / OTel CreateTimeSeries 'Points must be written in order'"
  combiner              = "OR"
  notification_channels = []
  severity              = "ERROR"

  conditions {
    display_name = "otel_points_out_of_order > 0 in 5 min"

    condition_threshold {
      filter = join(" AND ", [
        "resource.type = \"cloud_run_worker_pool\"",
        "metric.type = \"logging.googleapis.com/user/${google_logging_metric.otel_points_out_of_order.name}\"",
      ])
      comparison      = "COMPARISON_GT"
      threshold_value = 0
      duration        = "0s"

      aggregations {
        alignment_period   = "300s"
        per_series_aligner = "ALIGN_DELTA"
      }
    }
  }

  alert_strategy {
    auto_close = "1800s"
  }
}
