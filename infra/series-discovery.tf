# HOL-24 — Series catalog + discovery job.
#
# This file collocates every resource for the series-discovery feature:
# BQ table, CR Job, Scheduler trigger + scheduler-invoker IAM, new SA,
# IAM bindings, failure alert. Per the per-feature layout decision
# documented in internal design docs (not included in this public snapshot).
#
# Note: the SA name `ksh-series-discovery` is hardcoded here rather than
# added to infra/locals.tf::service_accounts. Audits of "all workload SAs"
# must grep `google_service_account` across infra/*.tf, not only
# infra/service-accounts.tf.

# ── BQ catalog table ──────────────────────────────────────────────────

resource "google_bigquery_table" "series_catalog" {
  dataset_id          = google_bigquery_dataset.kalshi_raw.dataset_id
  table_id            = "series_catalog"
  deletion_protection = false

  time_partitioning {
    type  = "DAY"
    field = "capture_ts"
    # expiration_ms intentionally unset → retain forever. Catalog history
    # is the point of this table (HOL-23 problem statement #2).
  }
  clustering = ["ticker"]

  schema = jsonencode([
    { name = "capture_ts", type = "TIMESTAMP", mode = "REQUIRED" },
    { name = "ticker", type = "STRING", mode = "REQUIRED" },
    { name = "title", type = "STRING" },
    { name = "category", type = "STRING" },
    { name = "frequency", type = "STRING" },
    { name = "tags", type = "STRING", mode = "REPEATED" },
    { name = "contract_url", type = "STRING" },
    { name = "raw_json", type = "JSON" },
    { name = "content_hash", type = "STRING", mode = "REQUIRED" },
    { name = "source", type = "STRING", mode = "REQUIRED" },
  ])

  labels = {
    feature = "series-discovery"
  }
}

# ── BQ market_catalog table (HOL-25) ───────────────────────────────────

resource "google_bigquery_table" "market_catalog" {
  dataset_id          = google_bigquery_dataset.kalshi_raw.dataset_id
  table_id            = "market_catalog"
  deletion_protection = false

  time_partitioning {
    type  = "DAY"
    field = "capture_ts"
    # expiration_ms intentionally unset → retain forever. Catalog history
    # is the point of this table (HOL-23 problem statement #2).
  }
  clustering = ["series_ticker", "market_ticker"]

  schema = jsonencode([
    { name = "capture_ts", type = "TIMESTAMP", mode = "REQUIRED" },
    { name = "market_ticker", type = "STRING", mode = "REQUIRED" },
    { name = "series_ticker", type = "STRING", mode = "REQUIRED" },
    { name = "title", type = "STRING" },
    { name = "status", type = "STRING" },
    { name = "open_time", type = "TIMESTAMP" },
    { name = "close_time", type = "TIMESTAMP" },
    { name = "expiration_time", type = "TIMESTAMP" },
    { name = "settlement_value", type = "STRING" },
    { name = "content_hash", type = "STRING", mode = "REQUIRED" },
  ])

  labels = {
    feature = "series-discovery"
  }
}

# ── Service account ───────────────────────────────────────────────────

resource "google_service_account" "series_discovery" {
  account_id   = "ksh-series-discovery"
  display_name = "Kalshi series-discovery job"
}

# ── IAM: workload bindings ────────────────────────────────────────────

resource "google_bigquery_dataset_iam_member" "series_discovery_data_editor" {
  dataset_id = google_bigquery_dataset.kalshi_raw.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.series_discovery.email}"
}

resource "google_project_iam_member" "series_discovery_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.series_discovery.email}"
}

resource "google_secret_manager_secret_iam_member" "series_discovery_secret" {
  secret_id = google_secret_manager_secret.kalshi_creds.secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.series_discovery.email}"
}

resource "google_project_iam_member" "series_discovery_log_writer" {
  project = var.project_id
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${google_service_account.series_discovery.email}"
}

resource "google_project_iam_member" "series_discovery_metric_writer" {
  project = var.project_id
  role    = "roles/monitoring.metricWriter"
  member  = "serviceAccount:${google_service_account.series_discovery.email}"
}

# ── Cloud Run Job ─────────────────────────────────────────────────────

resource "google_cloud_run_v2_job" "series_discovery" {
  name                = "series-discovery"
  location            = var.region
  deletion_protection = false

  template {
    template {
      service_account = google_service_account.series_discovery.email
      timeout         = var.series_discovery_timeout
      max_retries     = var.series_discovery_max_retries

      containers {
        image = var.series_discovery_image
        resources {
          limits = {
            cpu    = "1000m"
            memory = "512Mi"
          }
        }
        env {
          name  = "EXPORT_PROJECT_ID"
          value = var.project_id
        }
        env {
          name  = "EXPORT_TARGET_DATASET"
          value = google_bigquery_dataset.kalshi_raw.dataset_id
        }
        env {
          name  = "EXPORT_SERIES_CATALOG_TABLE"
          value = google_bigquery_table.series_catalog.table_id
        }
        env {
          name  = "EXPORT_MARKET_CATALOG_TABLE"
          value = google_bigquery_table.market_catalog.table_id
        }
        env {
          name  = "KALSHI_SERIES"
          value = var.kalshi_series
        }
        env {
          name  = "KALSHI_SECRET_NAME"
          value = google_secret_manager_secret.kalshi_creds.id
        }
        env {
          name  = "KALSHI_CONTROL_BUCKET"
          value = google_storage_bucket.archive.name
        }
        env {
          name  = "KALSHI_CONTROL_OBJECT"
          value = "control/series_desired.json"
        }
        env {
          name  = "EXPORT_VIEW_SERIES_SUBSCRIBED"
          value = google_bigquery_table.v_series_subscribed.table_id
        }
      }
    }
  }

  labels = {
    system = "kalshiflow"
    job    = "series-discovery"
  }
}

# ── Cloud Scheduler trigger (mirrors parquet-exporter wiring) ─────────

resource "google_cloud_run_v2_job_iam_member" "scheduler_invoker_series_discovery" {
  project  = var.project_id
  location = var.region
  name     = google_cloud_run_v2_job.series_discovery.name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.scheduler_invoker.email}"
}

resource "google_cloud_scheduler_job" "series_discovery" {
  name      = "kalshi-series-discovery"
  schedule  = var.series_discovery_schedule
  region    = var.region
  time_zone = "UTC"
  paused    = false

  http_target {
    uri         = "https://${var.region}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${var.project_id}/jobs/series-discovery:run"
    http_method = "POST"

    oauth_token {
      service_account_email = google_service_account.scheduler_invoker.email
    }
  }

  retry_config {
    retry_count          = var.series_discovery_retry_count
    min_backoff_duration = var.series_discovery_retry_backoff
  }
}

# ── Failure alert (mirror of parquet_exporter_failure) ────────────────

resource "google_monitoring_alert_policy" "series_discovery_failure" {
  display_name          = "kalshiflow / series-discovery run failed"
  combiner              = "OR"
  notification_channels = []
  severity              = "ERROR"

  conditions {
    display_name = "Cloud Run job execution failed"

    condition_threshold {
      filter = join(" AND ", [
        "resource.type = \"cloud_run_job\"",
        "resource.labels.job_name = \"series-discovery\"",
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

# ── HOL-26 slice 3: subscription event log ────────────────────────────

resource "google_bigquery_table" "subscription_log" {
  dataset_id          = google_bigquery_dataset.kalshi_raw.dataset_id
  table_id            = "subscription_log"
  deletion_protection = false

  time_partitioning {
    type  = "DAY"
    field = "ts"
    # expiration_ms intentionally unset → retain forever. The audit trail
    # of "why was this series subscribed and when" is the point of this
    # table (HOL-23 problem statement #2).
  }
  clustering = ["ticker"]

  schema = jsonencode([
    { name = "ts", type = "TIMESTAMP", mode = "REQUIRED" },
    { name = "ticker", type = "STRING", mode = "REQUIRED" },
    { name = "action", type = "STRING", mode = "REQUIRED" },
    { name = "reason", type = "STRING", mode = "REQUIRED" },
    { name = "actor", type = "STRING", mode = "REQUIRED" },
  ])

  labels = {
    feature = "series-discovery"
  }
}

resource "google_bigquery_table" "v_series_subscribed" {
  dataset_id          = google_bigquery_dataset.kalshi_raw.dataset_id
  table_id            = "v_series_subscribed"
  deletion_protection = false

  view {
    use_legacy_sql = false
    query          = <<-SQL
      WITH ranked AS (
        SELECT
          ticker,
          action,
          ts,
          ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY ts DESC) AS rn
        FROM `${var.project_id}.${google_bigquery_dataset.kalshi_raw.dataset_id}.${google_bigquery_table.subscription_log.table_id}`
        WHERE action IN ('subscribe', 'unsubscribe')
      )
      SELECT ticker
      FROM ranked
      WHERE rn = 1 AND action = 'subscribe'
      ORDER BY ticker
    SQL
  }

  labels = {
    feature = "series-discovery"
  }
}

# ── HOL-26 slice 3: GCS control-prefix IAM ────────────────────────────

resource "google_storage_bucket_iam_member" "discovery_control_object_admin" {
  bucket = google_storage_bucket.archive.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.series_discovery.email}"

  condition {
    title       = "control_prefix_only"
    description = "Discovery may only manage objects under control/."
    expression  = "resource.name.startsWith(\"projects/_/buckets/${google_storage_bucket.archive.name}/objects/control/\")"
  }
}

resource "google_storage_bucket_iam_member" "worker_control_object_viewer" {
  bucket = google_storage_bucket.archive.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.ws_worker.email}"

  condition {
    title       = "control_prefix_only"
    description = "ws-worker may only read objects under control/."
    expression  = "resource.name.startsWith(\"projects/_/buckets/${google_storage_bucket.archive.name}/objects/control/\")"
  }
}
