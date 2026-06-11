resource "google_pubsub_topic" "kalshi" {
  for_each = toset(local.streams)
  name     = "kalshi.${each.key}"

  message_retention_duration = "604800s" # 7 days

  schema_settings {
    schema   = google_pubsub_schema.envelope.id
    encoding = "BINARY"
  }

  labels = {
    stream = each.key
    system = "kalshiflow"
  }

  depends_on = [google_pubsub_schema.envelope]
}

resource "google_pubsub_topic" "deadletter" {
  name = "kalshi.deadletter"
  labels = {
    stream = "deadletter"
    system = "kalshiflow"
  }
}
