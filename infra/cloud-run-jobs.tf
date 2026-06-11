resource "google_cloud_run_v2_job" "parquet_exporter" {
  name                = "parquet-exporter"
  location            = var.region
  deletion_protection = false

  template {
    template {
      service_account = google_service_account.exporter.email
      timeout         = var.parquet_exporter_timeout
      max_retries     = var.parquet_exporter_max_retries

      containers {
        image = var.parquet_exporter_image
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
          name  = "EXPORT_KALSHI_DATASET"
          value = google_bigquery_dataset.kalshi_raw.dataset_id
        }
        env {
          name  = "EXPORT_GCS_BUCKET"
          value = google_storage_bucket.archive.name
        }
      }
    }
  }

  labels = {
    system = "kalshiflow"
    job    = "parquet-exporter"
  }
}
