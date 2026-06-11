resource "google_cloud_run_v2_worker_pool" "ws_worker" {
  name                = "ws-worker"
  location            = var.region
  deletion_protection = false # Plan 4 flips to true

  template {
    service_account = google_service_account.ws_worker.email

    # Kalshi WS worker. 1 vCPU / 512Mi is the gen2 always-allocated floor
    # (total CPU >= 1, total memory >= 512Mi). The old 2-container pool sat
    # exactly at that floor (2x 500m/256Mi); removing the kraken sidecar
    # (HOL-183) collapses to this single container at the same floor, so the
    # pool's billed resources are unchanged. HOL-183's real savings are
    # data-volume (Pub/Sub, BQ streaming inserts, GCS/BQ storage), not compute.
    containers {
      name  = "ws-worker"
      image = var.ws_worker_image
      resources {
        limits = {
          cpu    = "1"
          memory = "512Mi"
        }
      }
      env {
        name  = "KALSHI_PROJECT_ID"
        value = var.project_id
      }
      env {
        name  = "KALSHI_REGION"
        value = var.region
      }
      env {
        name  = "KALSHI_SECRET_NAME"
        value = "projects/${var.project_id}/secrets/${google_secret_manager_secret.kalshi_creds.secret_id}"
      }
      # Boot-time fallback when KALSHI_CONTROL_BUCKET object is absent (DR mode).
      # Live reconcile post-boot reads gs://${...}/control/series_desired.json
      # every 60 s (HOL-26 slice 3).
      env {
        name  = "KALSHI_SERIES"
        value = var.kalshi_series
      }
      env {
        name  = "KALSHI_WS_URL_PROD"
        value = "wss://api.elections.kalshi.com/trade-api/ws/v2"
      }
      env {
        name  = "KALSHI_CONTROL_BUCKET"
        value = google_storage_bucket.archive.name
      }
      env {
        name  = "KALSHI_CONTROL_OBJECT"
        value = "control/series_desired.json"
      }
    }
  }

  scaling {
    scaling_mode          = "MANUAL"
    manual_instance_count = var.ws_worker_instance_count
  }

  labels = {
    system = "kalshiflow"
  }
}
