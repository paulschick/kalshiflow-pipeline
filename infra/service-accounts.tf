resource "google_service_account" "ws_worker" {
  account_id   = local.service_accounts.ws_worker
  display_name = "Kalshi WS worker"
}

resource "google_service_account" "exporter" {
  account_id   = local.service_accounts.exporter
  display_name = "Kalshi parquet exporter job"
}

resource "google_service_account" "scheduler_invoker" {
  account_id   = "ksh-scheduler"
  display_name = "Cloud Scheduler invoker for Cloud Run jobs"
}
