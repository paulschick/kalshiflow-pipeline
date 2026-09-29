# kalshiflow

> **Public snapshot.** Scrubbed, single-commit snapshot of a private repository. All GCP identifiers (project
> id, bucket names, service-account emails) are replaced with placeholders such as `<your-gcp-project>`,
> `<your-archive-bucket>`, and `<your-tf-state-bucket>`; configure your own values via the Terraform variables in
> `infra/` (see `infra/terraform.tfvars.example`). The Parquet archive feeds the
> [kalshiflow-backtest](https://github.com/paulschick/kalshiflow-backtest) project.

Managed-services GCP pipeline that subscribes to Kalshi market WebSocket feeds and persists every orderbook,
trade, lifecycle, and settlement event to BigQuery (hot tier) and GCS Parquet (cold tier) for offline backtesting
and ML research. Built and operated solo; ran in production May–June 2026 and was then deliberately torn down.

- **Collection:** 16 Kalshi weather series (weather-only since 2026-06-10). A Kraken crypto ingester was built and
  later retired; its code remains in `cmd/kraken-ws-worker` and `internal/kraken/`.
- **Data path:** Kalshi WS → in-memory orderbook bookkeeper (1 s snapshots) → protobuf `Envelope` → Pub/Sub →
  BigQuery subscriptions → daily Parquet export to GCS.
- **Ops:** OpenTelemetry metrics to Cloud Monitoring, alert policies, dashboard, runbooks for every incident class
  seen in production.
- **Cost:** Cloud Run worker pool is the fixed-cost dominator; everything else sits under free-tier thresholds.
  Breakdown in `docs/reference/system.md` (Cost section).

## What's where

- **Reference:** `docs/reference/system.md`, `docs/reference/data-paths.md`, `docs/reference/ops.md`
- **Runbooks:** `docs/runbooks/`
- **Agent instructions:** `CLAUDE.md`
- **Source:**
  - `cmd/<binary>/main.go` — thin entry points (`ws-worker`, `parquet-exporter`, `series-discovery`,
    `decode-snapshot`)
  - `internal/<package>/` — implementation packages
  - `proto/kalshiflow/` — protobuf envelope + snapshot schemas (codegen via `protoc`)
  - `infra/` — Terraform; one resource family per `<family>.tf`
  - `Taskfile.yml` (build/test) + `Taskfile.ops.yml` (gcloud/bq/terraform shortcuts)
  - `scripts/` — one-off helpers (see `dump_kalshi_series.py`)

## Local development

```bash
task setup   # install Go tooling, lefthook, golangci-lint
task lint
task test
```

## Deploying to a fresh GCP project

Terraform does not enable APIs or create its own state bucket, and Cloud Run resources need images to exist
before they can be created, so a first deploy is ordered:

1. Create a project, link billing, and enable APIs:

   ```bash
   gcloud services enable run.googleapis.com pubsub.googleapis.com bigquery.googleapis.com \
     storage.googleapis.com secretmanager.googleapis.com artifactregistry.googleapis.com \
     cloudscheduler.googleapis.com monitoring.googleapis.com logging.googleapis.com \
     iam.googleapis.com --project=<project>
   ```

2. Create the state bucket `gs://<project>-tf-state`, then `PROJECT_ID=<project> task tf:init`.
3. Copy `infra/terraform.tfvars.example` to `infra/terraform.tfvars` and fill it in.
4. Create the Artifact Registry repo first:
   `terraform -chdir=infra apply -target=google_artifact_registry_repository.kalshi`.
5. Build and push images: the placeholder (`docker/placeholder/Dockerfile`, pushed by hand) and
   `task -y ops:image:deploy BIN=<bin> TAG=vX.Y.Z` for `ws-worker`, `parquet-exporter`, `series-discovery`.
   Set the image refs in `terraform.tfvars`.
6. `PROJECT_ID=<project> task tf:apply`.
7. Add a version to the `kalshi-creds` secret: JSON `{"api_key_id": "...", "private_key_pem": "..."}`.
8. Smoke-test: `task ops:worker:status`, `task ops:bq:rows`.
