# update-kalshi-series — DEPRECATED

> **Deprecated for routine use.** Use [`series-subscription`](series-subscription.md) for add/remove/list/discover operations. This runbook is retained only for the disaster-recovery (DR) cold-start case where both `series_desired.json` AND `subscription_log` are gone.

## When to use this runbook

Only when **both** of these are true:

- `gs://<your-archive-bucket>/control/series_desired.json` is missing or unreadable.
- `kalshi_raw.subscription_log` is empty or unrecoverable.

In every other situation — including a fresh project bootstrap where slice-3 substrate exists — use the routine runbook [`series-subscription`](series-subscription.md).

## Emergency tfvar fallback (DR only)

1. Edit `infra/terraform.tfvars`. Add or set `kalshi_series` to a comma-separated ticker list:

   ```diff
   -kalshi_series     = ""
   +kalshi_series     = "KXHIGHNY,KXHIGHLAX,KXHIGHCHI,KXHIGHMIA,KXHIGHDEN,KXHIGHTDAL,KXHIGHTPHX,KXHIGHTBOS,KXHIGHTSEA,KXHIGHPHIL,KXHIGHTDC,KXHIGHTHOU,KXHIGHAUS,KXHIGHTSFO,KXHIGHTMIN,KXRAINNYC"
   ```

   The current desired set is the 16 weather series (weather-only since 2026-06-10, HOL-183) — match
   whatever `v_series_subscribed` last held, not this example verbatim.

2. Apply:

   ```sh
   task tf:apply
   ```

   This rolls a new Cloud Run worker pool revision; the worker boots from the `KALSHI_SERIES` env because the GCS object is absent.

3. Verify the worker is healthy:

   ```sh
   task ops:worker:status
   ```

   Ready revision matches the just-applied one; condition is `Ready=True`.

4. Re-populate the event log so the GCS path becomes the source of truth again. For each ticker in the tfvar list:

   ```sh
   task ops:series:add SERIES=KXHIGHNY REASON="DR cold-start re-population"
   ```

5. Trigger discovery to materialize `series_desired.json` from `v_series_subscribed`:

   ```sh
   task ops:series:discover
   ```

6. After the next worker boot (or 60 s poll cycle on the running pod), the worker reads `series_desired.json` and the `KALSHI_SERIES` env path becomes inert. From this point on, routine subscription changes use [`series-subscription`](series-subscription.md).

## Why this file still exists

Incoming links from old commit messages, Linear comments, and external bookmarks need a discoverable landing page. Deleting the file would 404 those references. Keep this banner + the DR stub in place; do not extend the file with routine-use content.
