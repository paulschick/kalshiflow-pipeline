#!/usr/bin/env python3
"""
Probe Kalshi REST /markets endpoint to determine the canonical `status` filter
value that returns currently-tradable markets in a series.

Resolves Plan 2 OQ-2: slice 2.3 hard-codes `status=open` in
`internal/kalshi/markets.go`. If Kalshi's live API uses a different value
("active", "initialized", etc.), `GetOpenMarkets` returns an empty list and
the worker's roster is empty — discovery fails silently. This probe answers
"what status string does Kalshi actually return for tradable markets right now"
before slice 2.3 implementation locks the value into Go code.

Cadence:
  8 samples, 10 minutes apart, ~70 minutes total. Covers at least one full
  KXBTCD hourly market cycle so initialized → active → closed transitions
  are visible. For non-cycling series, fewer samples suffice — adjust SAMPLES
  and INTERVAL_S below.

Per sample:
  1. GET /markets?series_ticker=<series>                 (no status filter)
  2. GET /markets?series_ticker=<series>&status=open     (Plan 2.3's filter)
  3. Diff: tickers in (1) not in (2). If non-empty, the status filter is
     wrong — those markets are reachable via the unfiltered endpoint but not
     via the filter we picked. The verdict is the dominant status value in
     the diff set.

Output:
  tmp/probes/probe-status-<series>-<UTC>.json
  Human-readable summary printed to stdout each iteration.

Dependencies:
  pip install cryptography     (stdlib otherwise)

Setup:
  Edit API_KEY_ID and PEM_PATH below. The API key must have read scope on
  `markets`. Demo and prod both work; we run prod here per the project
  decision (see Plan 2 grilling OQ-5).
"""
import base64
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric.padding import MGF1, PSS

# --- Configuration -----------------------------------------------------------
#
# Defaults read from ~/.config/kalshiflow/{kalshi.prod.keyid,kalshi.prod.pem} —
# the project's local cred dir for read-only Kalshi auth. Override via env:
#   KALSHI_API_KEY_ID  — UUID-shaped key id (overrides keyid file)
#   KALSHI_PEM_PATH    — absolute path to PKCS8 PEM (overrides pem file)
#   KALSHI_SERIES      — series to probe (default KXBTCD)
#   KALSHI_PROBE_SAMPLES   — sample count (default 8)
#   KALSHI_PROBE_INTERVAL  — interval seconds (default 600)

_CRED_DIR = os.path.expanduser("~/.config/kalshiflow")

def _read_keyid_file() -> str:
    p = os.path.join(_CRED_DIR, "kalshi.prod.keyid")
    if os.path.exists(p):
        with open(p) as f:
            return f.read().strip()
    return "<KALSHI_API_KEY_ID>"

API_KEY_ID = os.environ.get("KALSHI_API_KEY_ID") or _read_keyid_file()
PEM_PATH   = os.environ.get("KALSHI_PEM_PATH")   or os.path.join(_CRED_DIR, "kalshi.prod.pem")

SERIES     = os.environ.get("KALSHI_SERIES", "KXBTCD")
BASE_URL   = "https://api.elections.kalshi.com"  # prod
SAMPLES    = int(os.environ.get("KALSHI_PROBE_SAMPLES", "8"))
INTERVAL_S = int(os.environ.get("KALSHI_PROBE_INTERVAL", "600"))

# The filter values to probe. Each sample queries the unfiltered endpoint
# plus each of these. The script reports which filter (if any) returns the
# same set as the unfiltered endpoint minus terminal states (settled/closed).
CANDIDATE_FILTERS = ["open", "active", "initialized", "open,active"]

# --- Implementation ----------------------------------------------------------

def load_key():
    with open(PEM_PATH, "rb") as f:
        return serialization.load_pem_private_key(f.read(), password=None)

def sign(key, ts: str, method: str, path: str) -> str:
    """RSA-PSS over (timestamp || method || path) with salt length = digest length."""
    msg = (ts + method + path).encode("utf-8")
    sig = key.sign(
        msg,
        PSS(mgf=MGF1(hashes.SHA256()), salt_length=PSS.DIGEST_LENGTH),
        hashes.SHA256(),
    )
    return base64.b64encode(sig).decode("ascii")

def get(key, path: str) -> dict:
    ts = str(int(time.time() * 1000))
    sig_path = path.split("?", 1)[0]  # Kalshi signs the path, not the query
    headers = {
        "KALSHI-ACCESS-KEY":       API_KEY_ID,
        "KALSHI-ACCESS-SIGNATURE": sign(key, ts, "GET", sig_path),
        "KALSHI-ACCESS-TIMESTAMP": ts,
    }
    req = urllib.request.Request(BASE_URL + path, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"HTTP {e.code} on {path}: {body}") from e

def list_markets(key, series: str, status: str | None = None, max_pages: int = 5) -> list[dict]:
    rows: list[dict] = []
    cursor = ""
    for _ in range(max_pages):
        params: dict[str, str] = {"series_ticker": series, "limit": "1000"}
        if status:
            params["status"] = status
        if cursor:
            params["cursor"] = cursor
        page = get(key, "/trade-api/v2/markets?" + urllib.parse.urlencode(params))
        rows.extend(page.get("markets", []))
        cursor = page.get("cursor", "")
        if not cursor:
            break
    return rows

def histogram(rows: list[dict]) -> dict[str, int]:
    h: dict[str, int] = {}
    for r in rows:
        s = r.get("status", "<unknown>")
        h[s] = h.get(s, 0) + 1
    return h

def take_sample(key, series: str) -> dict:
    unfiltered = list_markets(key, series)
    by_filter: dict[str, dict] = {}
    for f in CANDIDATE_FILTERS:
        try:
            rows = list_markets(key, series, status=f)
            by_filter[f] = {
                "count":           len(rows),
                "unique_tickers":  sorted({m.get("ticker") for m in rows if m.get("ticker")}),
                "error":           None,
            }
        except Exception as e:  # noqa: BLE001 — record per-filter failure (e.g. HTTP 400 invalid filter)
            by_filter[f] = {
                "count":           None,
                "unique_tickers":  [],
                "error":           str(e),
            }
    u_set = {m.get("ticker") for m in unfiltered if m.get("ticker")}
    diffs: dict[str, list[str]] = {}
    for f, info in by_filter.items():
        diffs[f] = sorted(u_set - set(info["unique_tickers"]))
    return {
        "ts":                            time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "unfiltered_count":              len(unfiltered),
        "unfiltered_status_histogram":   histogram(unfiltered),
        "filtered_counts":               {f: by_filter[f]["count"] for f in CANDIDATE_FILTERS},
        "filtered_errors":               {f: by_filter[f]["error"] for f in CANDIDATE_FILTERS},
        "tickers_in_unfiltered_not_in":  {f: diffs[f][:50] for f in CANDIDATE_FILTERS},
    }

def main() -> int:
    if API_KEY_ID.startswith("<") or PEM_PATH.startswith("<"):
        print("ERROR: edit API_KEY_ID and PEM_PATH at the top of the script.", file=sys.stderr)
        return 2

    key = load_key()

    samples: list[dict] = []
    for i in range(SAMPLES):
        try:
            s = take_sample(key, SERIES)
            samples.append(s)
            print(
                f"[{i+1}/{SAMPLES}] {s['ts']}  "
                f"unfiltered={s['unfiltered_count']}  "
                f"by_filter={s['filtered_counts']}  "
                f"hist={s['unfiltered_status_histogram']}"
            )
        except Exception as e:  # noqa: BLE001 — probe; capture all failure modes
            err = {"ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "error": str(e)}
            samples.append(err)
            print(f"[{i+1}/{SAMPLES}] ERROR: {e}", file=sys.stderr)

        if i < SAMPLES - 1:
            time.sleep(INTERVAL_S)

    out_dir = "tmp/probes"
    os.makedirs(out_dir, exist_ok=True)
    stamp = time.strftime("%Y-%m-%dT%H-%M", time.gmtime())
    out_path = os.path.join(out_dir, f"probe-status-{SERIES}-{stamp}.json")
    with open(out_path, "w") as f:
        json.dump(
            {
                "series":             SERIES,
                "base_url":           BASE_URL,
                "candidate_filters":  CANDIDATE_FILTERS,
                "samples":            samples,
            },
            f,
            indent=2,
        )

    print(f"\nSamples saved to {out_path}")
    print(
        "\nVerdict: examine `unfiltered_status_histogram` to see what statuses\n"
        "Kalshi returns. Then examine `filtered_counts` — the candidate filter\n"
        "that produces a non-empty result and zero `tickers_in_unfiltered_not_in`\n"
        "for non-terminal statuses (i.e. excluding settled/closed) is the\n"
        "correct value for `q.Set(\"status\", ...)` in internal/kalshi/markets.go.\n"
    )
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
