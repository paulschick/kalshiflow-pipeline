data "google_project" "this" {}

locals {
  pubsub_sa = "service-${data.google_project.this.number}@gcp-sa-pubsub.iam.gserviceaccount.com"
}

resource "google_bigquery_dataset_iam_member" "pubsub_data_editor" {
  dataset_id = google_bigquery_dataset.kalshi_raw.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${local.pubsub_sa}"
}

resource "google_pubsub_subscription" "bq" {
  for_each = toset(local.streams)
  name     = "kalshi.${each.key}.bq"
  topic    = google_pubsub_topic.kalshi[each.key].id

  ack_deadline_seconds       = 60
  message_retention_duration = "604800s" # 7 days

  bigquery_config {
    table               = "${var.project_id}.${google_bigquery_dataset.kalshi_raw.dataset_id}.${local.bq_table_for_stream[each.key]}"
    use_topic_schema    = true
    write_metadata      = false
    drop_unknown_fields = false
  }

  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "600s"
  }

  dead_letter_policy {
    dead_letter_topic     = google_pubsub_topic.deadletter.id
    max_delivery_attempts = 10
  }

  depends_on = [
    google_bigquery_dataset_iam_member.pubsub_data_editor,
    google_bigquery_table.stream,
  ]
}
