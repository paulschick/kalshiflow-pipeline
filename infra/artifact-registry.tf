resource "google_artifact_registry_repository" "kalshi" {
  location      = var.region
  repository_id = local.ar_repo
  description   = "kalshiflow container images"
  format        = "DOCKER"
}
