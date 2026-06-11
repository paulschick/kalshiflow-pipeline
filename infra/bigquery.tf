resource "google_bigquery_dataset" "kalshi_raw" {
  dataset_id                 = "kalshi_raw"
  location                   = var.bq_dataset_location
  description                = "Raw Kalshi event streams ingested via Pub/Sub → BQ subscriptions."
  delete_contents_on_destroy = true

  labels = {
    tier = "warm"
  }
}

locals {
  # Source of truth lives at internal/envelope/schema.json so the Go
  # envelope contract (slice 1.4) and the BQ table schema can never drift.
  bq_envelope_schema = file("${path.module}/../internal/envelope/schema.json")
}

resource "google_bigquery_table" "stream" {
  for_each                 = toset(local.streams)
  dataset_id               = google_bigquery_dataset.kalshi_raw.dataset_id
  table_id                 = local.bq_table_for_stream[each.key]
  schema                   = local.bq_envelope_schema
  deletion_protection      = false
  require_partition_filter = true

  time_partitioning {
    type          = "DAY"
    field         = "event_ts"
    expiration_ms = local.partition_expiration_ms_for_stream[each.key]
  }

  clustering = ["series_id", "contract_id"]

  labels = {
    stream = each.key
  }
}
