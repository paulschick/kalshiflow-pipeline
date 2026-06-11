variable "project_id" {
  description = "GCP project ID."
  type        = string
}

variable "region" {
  description = "Default region for regional resources."
  type        = string
  default     = "us-east4"
}

variable "bq_dataset_location" {
  description = "BigQuery dataset location (multi-region or region)."
  type        = string
  default     = "us-east4"
}

variable "gcs_archive_storage_class" {
  description = "Initial storage class for the archive bucket (used in slice 1.3)."
  type        = string
  default     = "STANDARD"
}

variable "placeholder_image" {
  description = "Placeholder container image used by Cloud Run worker pool and jobs (set in slice 1.3)."
  type        = string
  default     = "unset"
}

variable "kalshi_series" {
  description = "Comma-separated list of Kalshi series tickers the WS worker will subscribe to (Plan 2 consumes this)."
  type        = string
  default     = "KXBTCD"
}

variable "alert_email" {
  description = "Email address for non-paging alerts (Plan 4 will wire this up; Plan 1 only stores it)."
  type        = string
  default     = ""
}

variable "parquet_exporter_timeout" {
  description = "Cloud Run job timeout for parquet-exporter (Go duration string)."
  type        = string
  default     = "1800s"
}

variable "parquet_exporter_max_retries" {
  description = "Cloud Run job task max_retries for parquet-exporter."
  type        = number
  default     = 1
}

variable "parquet_exporter_schedule" {
  description = "Cron expression for the parquet-exporter Cloud Scheduler trigger."
  type        = string
  default     = "0 1 * * *" # daily at 01:00 UTC
}

variable "parquet_exporter_retry_count" {
  description = "Cloud Scheduler retry_count for the parquet-exporter trigger."
  type        = number
  default     = 3
}

variable "parquet_exporter_retry_backoff" {
  description = "Cloud Scheduler min_backoff_duration for the parquet-exporter trigger."
  type        = string
  default     = "30s"
}

variable "ws_worker_image" {
  description = "Container image (with tag) for the ws-worker pool. Set by slice 2.1b once the binary is pushed to Artifact Registry."
  type        = string
  default     = "unset"
}

variable "ws_worker_instance_count" {
  description = "Cloud Run worker-pool manual instance count. 1 = live; 0 = idle (post-slice-1.4 idle-cost toggle, also used by the rollback recipe)."
  type        = number
  default     = 1
}

variable "parquet_exporter_image" {
  description = "Container image (with tag) for the parquet-exporter Cloud Run job."
  type        = string
  default     = "unset"
}

variable "series_discovery_image" {
  description = "Container image (with tag) for the series-discovery Cloud Run job."
  type        = string
  default     = "unset"
}

variable "series_discovery_timeout" {
  description = "Cloud Run job timeout for series-discovery (Go duration string)."
  type        = string
  default     = "300s"
}

variable "series_discovery_max_retries" {
  description = "Cloud Run job task max_retries for series-discovery."
  type        = number
  default     = 1
}

variable "series_discovery_schedule" {
  description = "Cron expression for the series-discovery Cloud Scheduler trigger."
  type        = string
  default     = "0 */6 * * *" # every 6h
}

variable "series_discovery_retry_count" {
  description = "Cloud Scheduler retry_count for the series-discovery trigger."
  type        = number
  default     = 3
}

variable "series_discovery_retry_backoff" {
  description = "Cloud Scheduler min_backoff_duration for the series-discovery trigger."
  type        = string
  default     = "30s"
}
