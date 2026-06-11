resource "google_secret_manager_secret" "kalshi_creds" {
  secret_id = "kalshi-creds"

  replication {
    user_managed {
      replicas {
        location = var.region
      }
    }
  }

  labels = {
    system = "kalshiflow"
  }
}
