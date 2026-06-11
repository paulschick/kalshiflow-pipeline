resource "google_pubsub_topic_iam_member" "deadletter_publisher" {
  topic  = google_pubsub_topic.deadletter.id
  role   = "roles/pubsub.publisher"
  member = "serviceAccount:${local.pubsub_sa}"
}

resource "google_pubsub_subscription_iam_member" "stream_sub_subscriber" {
  for_each     = google_pubsub_subscription.bq
  subscription = each.value.id
  role         = "roles/pubsub.subscriber"
  member       = "serviceAccount:${local.pubsub_sa}"
}
