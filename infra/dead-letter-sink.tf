resource "google_bigquery_table" "dead_letter_events" {
  dataset_id               = google_bigquery_dataset.kalshi_raw.dataset_id
  table_id                 = "dead_letter_events"
  deletion_protection      = false
  require_partition_filter = true

  schema = jsonencode([
    { name = "subscription_name", type = "STRING", mode = "REQUIRED" },
    { name = "message_id", type = "STRING", mode = "REQUIRED" },
    { name = "publish_time", type = "TIMESTAMP", mode = "REQUIRED" },
    { name = "data", type = "STRING", mode = "NULLABLE" },
    { name = "attributes", type = "JSON", mode = "NULLABLE" },
  ])

  time_partitioning {
    type          = "DAY"
    field         = "publish_time"
    expiration_ms = 1209600000 # 14 days
  }

  labels = {
    system = "kalshiflow"
    stream = "deadletter"
  }
}

resource "google_pubsub_subscription" "deadletter_bq" {
  name  = "kalshi.deadletter.bq"
  topic = google_pubsub_topic.deadletter.id

  ack_deadline_seconds       = 60
  message_retention_duration = "604800s" # 7 days

  # write_metadata=true alone fixes the schema to (subscription_name, message_id, publish_time,
  # data, attributes) and writes the raw message body verbatim into `data`. Combining it with
  # use_table_schema=true makes Pub/Sub also try to map JSON body fields onto table columns,
  # which silently rejects every DLQ row whose body doesn't match the metadata schema.
  bigquery_config {
    table          = "${var.project_id}.${google_bigquery_dataset.kalshi_raw.dataset_id}.${google_bigquery_table.dead_letter_events.table_id}"
    write_metadata = true
  }

  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "600s"
  }

  depends_on = [
    google_bigquery_dataset_iam_member.pubsub_data_editor,
    google_bigquery_table.dead_letter_events,
  ]
}
