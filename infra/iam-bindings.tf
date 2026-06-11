# ws-worker: publish to all stream topics, read kalshi-creds, write logs+metrics
resource "google_pubsub_topic_iam_member" "ws_worker_publisher" {
  for_each = google_pubsub_topic.kalshi
  topic    = each.value.id
  role     = "roles/pubsub.publisher"
  member   = "serviceAccount:${google_service_account.ws_worker.email}"
}

resource "google_secret_manager_secret_iam_member" "ws_worker_secret" {
  secret_id = google_secret_manager_secret.kalshi_creds.secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.ws_worker.email}"
}

resource "google_project_iam_member" "ws_worker_log_writer" {
  project = var.project_id
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${google_service_account.ws_worker.email}"
}

resource "google_project_iam_member" "ws_worker_metric_writer" {
  project = var.project_id
  role    = "roles/monitoring.metricWriter"
  member  = "serviceAccount:${google_service_account.ws_worker.email}"
}

# exporter: BQ data viewer + job user, GCS object admin on archive, log writer
resource "google_bigquery_dataset_iam_member" "exporter_data_viewer" {
  dataset_id = google_bigquery_dataset.kalshi_raw.dataset_id
  role       = "roles/bigquery.dataViewer"
  member     = "serviceAccount:${google_service_account.exporter.email}"
}

resource "google_project_iam_member" "exporter_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.exporter.email}"
}

resource "google_storage_bucket_iam_member" "exporter_object_admin" {
  bucket = google_storage_bucket.archive.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.exporter.email}"
}

resource "google_project_iam_member" "exporter_log_writer" {
  project = var.project_id
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${google_service_account.exporter.email}"
}

resource "google_project_iam_member" "exporter_metric_writer" {
  project = var.project_id
  role    = "roles/monitoring.metricWriter"
  member  = "serviceAccount:${google_service_account.exporter.email}"
}
