resource "google_pubsub_schema" "envelope" {
  name       = "kalshi-envelope-v1"
  type       = "PROTOCOL_BUFFER"
  definition = file("${path.module}/../proto/kalshiflow/envelope/v1/envelope.proto")

  lifecycle {
    create_before_destroy = true
  }
}
