resource "google_cloud_run_v2_job_iam_member" "scheduler_invoker" {
  project  = var.project_id
  location = var.region
  name     = google_cloud_run_v2_job.parquet_exporter.name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.scheduler_invoker.email}"
}

resource "google_cloud_scheduler_job" "parquet_exporter" {
  name      = "kalshi-parquet-exporter"
  schedule  = var.parquet_exporter_schedule
  region    = var.region
  time_zone = "UTC"
  paused    = false # unpaused by Plan 2.8 (parquet-exporter live)

  http_target {
    uri         = "https://${var.region}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${var.project_id}/jobs/parquet-exporter:run"
    http_method = "POST"

    oauth_token {
      service_account_email = google_service_account.scheduler_invoker.email
    }
  }

  retry_config {
    retry_count          = var.parquet_exporter_retry_count
    min_backoff_duration = var.parquet_exporter_retry_backoff
  }
}
