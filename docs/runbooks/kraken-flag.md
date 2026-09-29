# Runbook — kraken-flag kill switch

> **RETIRED 2026-06-10.** Kraken collection was disabled via this flag at 2026-06-10T11:19:10Z
> (`kraken_enabled.json` generation), then the flag, worker container, and `ops:kraken:*` tasks were removed
> with the weather-only pivot. Kept as historical reference only.

## What it gates

`control/kraken_enabled.json` in `gs://${PROJECT_ID}-kalshi-archive/` controls whether the Kraken WS worker
(not yet deployed at the time of writing) bootstraps and subscribes. When `enabled=false`, the worker
exits 0 at boot and the worker pool stays warm but does no work. When `enabled=true`, the worker subscribes
to its configured pair set and emits to `kraken.*` topics.

The flag is Kraken-only. It does not affect the Kalshi `ws-worker`.

## How to flip it

```bash
# Status (what's currently set).
task ops:kraken:status

# Turn Kraken collection OFF. REASON is required and surfaces in worker logs.
task -y ops:kraken:off REASON="incident: kraken WS stall blocking ingest"

# Turn it back ON.
task -y ops:kraken:on REASON="resumed after upstream recovery"
```

`task -y` is required from non-TTY callers (CI, scripts, Claude Code's Bash tool) because the `on`/`off`
tasks use a `prompt:` confirmation. Interactive shells can omit `-y` and confirm at the prompt.

`REASON` is mandatory — both as operational hygiene and because the value is embedded in the JSON body and
shows up in `kraken_flag_changed` slog lines downstream.

## How it propagates

The worker polls the GCS object every 60s ± 10s jitter. A flip therefore reaches a live worker within
~70s in the worst case. The poller uses `obj.Attrs().Generation` for cheap-unchanged checks; body bytes
are only read on a generation change.

Default semantics:

| Failure mode | Behaviour |
|---|---|
| Object absent at first boot (never-loaded) | Treat as `enabled=true` (default-on). Worker logs INFO once. |
| Object absent after a prior successful load | Retain last-known value; log WARN. |
| Transient GCS error (5xx / network / IAM denied) | Retain last-known; log WARN after ≥2 consecutive failures. |
| Body parse fail | Retain last-known; log ERROR. **Operator must republish to recover.** |

## Audit trail

Three sources, in order of usefulness:

1. **`kraken_flag_changed` slog lines** in the consumer (`cmd/kraken-flag-smoke` today; `cmd/kraken-ws-worker`
   once the Kraken worker ships). Carries the `reason`, `actor`, `generation` ULID, and the new `enabled` value from the
   body.
2. **Cloud Audit Logs (Storage Admin Activity)** — every write to the object records the principal
   (`gcloud config get-value account` at task time) plus the API call.
3. **Last-known body** via `task ops:kraken:status` — shows `reason` / `actor` / `generated_at` for the
   currently-published value.

Bucket versioning is intentionally **off**. Prior bodies are not retained on overwrite; the audit log is the
canonical timeline of flips.

## Smoke harness

`cmd/kraken-flag-smoke` is a local CLI that runs the same poll loop without subscribing to Kraken.

```bash
export KRAKEN_FLAG_BUCKET=${PROJECT_ID}-kalshi-archive
go run ./cmd/kraken-flag-smoke
# → logs kraken_flag=<bool> at boot and on every transition. Ctrl-C to stop.
```

Useful when validating an operator flip from a development machine before relying on the (not-yet-deployed)
worker poller.

## Recovery from accidental delete

The Terraform seed (`infra/kraken-flag.tf`) carries `lifecycle { ignore_changes = [content, ...] }` so a
re-apply does **not** clobber operator flips. To recover from an accidental delete:

```bash
task -y tf:apply:target TARGET=google_storage_bucket_object.kraken_enabled_seed
```

This recreates the seed object with `enabled=true` and `reason="initial seed"`. Follow up with an explicit
`task -y ops:kraken:on REASON="recover from accidental delete"` so the body carries a real reason and the
audit log records the recovery actor.

## Refs

- `internal/featureflag/kraken.go` — implementation.
- `internal/worker/desiredset.go` — reference poll-loop pattern.
- Memory `feedback_task_y_for_non_tty` — why `task -y` is required outside a TTY.
