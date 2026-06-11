provider "google" {
  project = var.project_id
  region  = var.region

  # Provider-level labels propagate to every resource that supports labels.
  # Resources may add their own (e.g., stream, tier, job) — they merge.
  # Provider 7.x writes these into a shadow `terraform_labels` attribute,
  # so plans show `terraform_labels = { system = "kalshiflow" }` on every
  # resource that supports labels — that is expected, not drift.
  default_labels = {
    system = "kalshiflow"
  }
}
