# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "cryptography",
# ]
# ///
"""
Probe Kalshi REST sweep behavior for the discovery-gap fix.

Two snapshots, ~120s apart, of /trade-api/v2/markets?series_ticker=X&status=open&limit=1000
for each configured series. Captures:

  - market count + page count + wall latency per call
  - distinct response.status values in the result set
  - sample of 5 tickers
  - delta between snapshots: tickers added, tickers removed (sweep diff signal — what
    the worker's sweep loop will use to drive subscribe/unsubscribe)

Output written to tmp/probes/probe-rest-discovery-<UTC>.json + stdout summary.

Reads creds from ~/.config/kalshiflow/{kalshi.prod.keyid,kalshi.prod.pem}; matches
the auth pattern of probe-kalshi-status.py.

Run:
  uv run scripts/probe-kalshi-rest-sweep.py
  KALSHI_REST_PROBE_GAP_S=60 uv run scripts/probe-kalshi-rest-sweep.py
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

CRED_DIR = os.path.expanduser("~/.config/kalshiflow")


def _read_keyid_file() -> str:
    p = os.path.join(CRED_DIR, "kalshi.prod.keyid")
    if os.path.exists(p):
        with open(p) as f:
            return f.read().strip()
    return "<KALSHI_API_KEY_ID>"


API_KEY_ID = os.environ.get("KALSHI_API_KEY_ID") or _read_keyid_file()
PEM_PATH = os.environ.get("KALSHI_PEM_PATH") or os.path.join(CRED_DIR, "kalshi.prod.pem")
BASE_URL = os.environ.get("KALSHI_REST_BASE", "https://api.elections.kalshi.com")
SERIES = [s.strip() for s in os.environ.get("KALSHI_SERIES", "KXBTCD,KXETHD").split(",") if s.strip()]
GAP_S = int(os.environ.get("KALSHI_REST_PROBE_GAP_S", "120"))
LIMIT = int(os.environ.get("KALSHI_REST_PROBE_LIMIT", "1000"))


def load_key():
    with open(PEM_PATH, "rb") as f:
        return serialization.load_pem_private_key(f.read(), password=None)


def sign(key, ts: str, method: str, path: str) -> str:
    msg = (ts + method + path).encode("utf-8")
    sig = key.sign(
        msg,
        PSS(mgf=MGF1(hashes.SHA256()), salt_length=PSS.DIGEST_LENGTH),
        hashes.SHA256(),
    )
    return base64.b64encode(sig).decode("ascii")


def get(key, path: str) -> dict:
    ts = str(int(time.time() * 1000))
    sig_path = path.split("?", 1)[0]
    headers = {
        "KALSHI-ACCESS-KEY": API_KEY_ID,
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


def sweep_series(key, series: str) -> dict:
    """Run one full enumeration of currently-open markets for a series."""
    started = time.monotonic()
    pages = 0
    cursor = ""
    markets: list[dict] = []
    page_latencies_ms: list[float] = []

    while True:
        params: dict[str, str] = {"series_ticker": series, "status": "open", "limit": str(LIMIT)}
        if cursor:
            params["cursor"] = cursor
        page_started = time.monotonic()
        page = get(key, "/trade-api/v2/markets?" + urllib.parse.urlencode(params))
        page_latencies_ms.append((time.monotonic() - page_started) * 1000)
        rows = page.get("markets", [])
        markets.extend(rows)
        pages += 1
        cursor = page.get("cursor", "")
        if not cursor:
            break
        if pages > 20:
            print(f"WARN: {series} exceeded 20 pages, stopping", file=sys.stderr)
            break

    elapsed_ms = (time.monotonic() - started) * 1000
    statuses: dict[str, int] = {}
    market_types: dict[str, int] = {}
    for m in markets:
        s = m.get("status", "<missing>")
        statuses[s] = statuses.get(s, 0) + 1
        mt = m.get("market_type", "<missing>")
        market_types[mt] = market_types.get(mt, 0) + 1

    sample = markets[0] if markets else None
    return {
        "series": series,
        "wall_clock": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "pages": pages,
        "elapsed_ms": round(elapsed_ms, 1),
        "page_latencies_ms": [round(x, 1) for x in page_latencies_ms],
        "market_count": len(markets),
        "tickers": sorted({m.get("ticker") for m in markets if m.get("ticker")}),
        "status_histogram": statuses,
        "market_type_histogram": market_types,
        "sample_market": sample,
    }


def diff(a: list[str], b: list[str]) -> dict:
    sa, sb = set(a), set(b)
    return {
        "added_in_second_sweep": sorted(sb - sa),
        "removed_in_second_sweep": sorted(sa - sb),
        "stable": len(sa & sb),
    }


def main() -> int:
    if API_KEY_ID.startswith("<"):
        print("ERROR: missing keyid file or KALSHI_API_KEY_ID env", file=sys.stderr)
        return 2

    key = load_key()

    print(f"REST sweep probe — series={SERIES} gap={GAP_S}s base={BASE_URL}")
    snapshots: list[dict] = []
    for n in (1, 2):
        print(f"\n--- Snapshot {n} ---")
        snap = []
        for s in SERIES:
            r = sweep_series(key, s)
            snap.append(r)
            print(f"  {s}: count={r['market_count']:5d} pages={r['pages']} elapsed={r['elapsed_ms']:.0f}ms statuses={r['status_histogram']}")
        snapshots.append(snap)
        if n == 1:
            print(f"  (sleeping {GAP_S}s before snapshot 2)")
            time.sleep(GAP_S)

    # Per-series diff between the two snapshots.
    diffs: dict[str, dict] = {}
    for i, s in enumerate(SERIES):
        diffs[s] = diff(snapshots[0][i]["tickers"], snapshots[1][i]["tickers"])

    out = {
        "base_url": BASE_URL,
        "gap_s": GAP_S,
        "series": SERIES,
        "snapshots": snapshots,
        "diffs_between_snapshots": diffs,
    }

    # Strip tickers list from on-disk output to keep it readable; keep counts.
    on_disk = json.loads(json.dumps(out))
    for snap in on_disk["snapshots"]:
        for entry in snap:
            entry["tickers_sample_first_10"] = entry["tickers"][:10]
            entry["tickers_count"] = len(entry["tickers"])
            del entry["tickers"]

    out_dir = "tmp/probes"
    os.makedirs(out_dir, exist_ok=True)
    stamp = time.strftime("%Y-%m-%dT%H-%M", time.gmtime())
    out_path = os.path.join(out_dir, f"probe-rest-discovery-{stamp}.json")
    with open(out_path, "w") as f:
        json.dump(on_disk, f, indent=2)

    print(f"\n=== Written to {out_path} ===\n")
    print("Diffs between snapshot 1 and snapshot 2:")
    for s, d in diffs.items():
        print(f"  {s}: +{len(d['added_in_second_sweep'])} -{len(d['removed_in_second_sweep'])} stable={d['stable']}")
        if d["added_in_second_sweep"]:
            print(f"    added: {d['added_in_second_sweep'][:5]}{'...' if len(d['added_in_second_sweep']) > 5 else ''}")
        if d["removed_in_second_sweep"]:
            print(f"    removed: {d['removed_in_second_sweep'][:5]}{'...' if len(d['removed_in_second_sweep']) > 5 else ''}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
