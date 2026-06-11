terraform {
  backend "gcs" {
    bucket = "<your-tf-state-bucket>"
    prefix = "infra"
    # CLI `-backend-config="bucket=..."` still overrides these at init time.
  }
}
