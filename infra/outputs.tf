output "project_id" {
  value = var.project_id
}

output "region" {
  value = var.region
}

output "topic_names" {
  value = { for s in local.streams : s => google_pubsub_topic.kalshi[s].name }
}

output "bq_dataset" {
  value = google_bigquery_dataset.kalshi_raw.dataset_id
}

output "kalshi_series" {
  description = "WS worker subscribed series; consumed by slice 1.3c Task 2 verification."
  value       = var.kalshi_series
}

output "monitoring_dashboard_id" {
  description = "Resource ID of the kalshiflow Cloud Monitoring dashboard. Consumed by `ops:dashboard:open`."
  value       = google_monitoring_dashboard.main.id
}
