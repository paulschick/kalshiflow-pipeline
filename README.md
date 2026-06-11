# kalshiflow

> **Public snapshot.** This is a scrubbed snapshot of a private repository, published as
> reference architecture for the blog series accompanying the
> [kalshiflow-backtest](https://github.com/paulschick/kalshiflow-backtest) project. All GCP
> identifiers (project id, bucket names, service-account emails) have been replaced with
> placeholders such as `<your-gcp-project>`, `<your-archive-bucket>`, and `<your-tf-state-bucket>`;
> configure your own values via the Terraform variables in `infra/` (see
> `infra/terraform.tfvars.example`). No history is included — this is a single squashed commit.

Managed-services GCP pipeline subscribing to Kalshi market WebSocket feeds and persisting events to BigQuery (hot tier)
and GCS Parquet (cold tier) for offline backtesting and ML research. Weather-only collection (16 weather series) as of
2026-06-10 (HOL-183); the crypto/Kraken side is retired. Solo developer (Cloud Run worker pool is the fixed-cost
dominator; everything else under free-tier thresholds). Target prod regime is 10-20 concurrent series. Cost breakdown:
see `docs/reference/system.md` Cost section.

## What's where

- **Reference (LLM-agent / future-you):** `docs/reference/system.md`, `docs/reference/data-paths.md`,
  `docs/reference/ops.md`
- **Agent instructions:** `CLAUDE.md`
- **Forward-looking work (needs re-planning):** `docs/next-steps.md`
- **Source:**
  - `cmd/<binary>/main.go` — thin entry points (`ws-worker`, `parquet-exporter`, `decode-snapshot`)
  - `internal/<package>/` — implementation packages
  - `proto/kalshiflow/` — protobuf envelope + snapshot schemas (codegen via `protoc`)
  - `infra/` — Terraform; one resource family per `<family>.tf`
  - `Taskfile.yml` (build/test) + `Taskfile.ops.yml` (gcloud/bq/terraform shortcuts)
  - `scripts/` — one-off helpers (see `dump_kalshi_series.py`)

## Quickstart

```bash
task setup     # install Go tooling, lefthook, golangci-lint
task tf:init   # initialise Terraform
task tf:apply  # apply infrastructure
```
