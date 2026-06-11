# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "cryptography",
#   "websockets",
# ]
# ///
"""
Probe Kalshi market_lifecycle_v2 WS channel.

Connects to the prod WS endpoint, subscribes to the exchange-wide
`market_lifecycle_v2` channel, captures every frame for a window, and
emits:

  1. Per-(frame_type, event_type) row count.
  2. Per event_type, the union of `msg` keys observed and 1 sample payload
     trimmed to those keys.
  3. Per event_type, count broken down by series prefix (KXBTCD / KXETHD /
     other) so we can tell whether `activated` fires for our series.

Output written to tmp/probes/probe-lifecycle-<UTC>.json + stdout summary.

Reads creds from ~/.config/kalshiflow/{kalshi.prod.keyid,kalshi.prod.pem}
(matches scripts/probe-kalshi-status.py). Override via env:
  KALSHI_API_KEY_ID, KALSHI_PEM_PATH
  KALSHI_LIFECYCLE_PROBE_S    (default 600 = 10 min)
  KALSHI_WS_URL               (default wss://api.elections.kalshi.com/trade-api/ws/v2)
  KALSHI_LIFECYCLE_SERIES     (comma-separated; default "KXBTCD,KXETHD" — used only for the
                               per-series rollup, not for filtering on the wire)

Run:
  uv run scripts/probe-kalshi-lifecycle.py
  KALSHI_LIFECYCLE_PROBE_S=300 uv run scripts/probe-kalshi-lifecycle.py
"""

import asyncio
import base64
import json
import os
import sys
import time
from collections import defaultdict
from typing import Any

import websockets
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
WS_URL = os.environ.get("KALSHI_WS_URL", "wss://api.elections.kalshi.com/trade-api/ws/v2")
DURATION_S = int(os.environ.get("KALSHI_LIFECYCLE_PROBE_S", "600"))
SERIES = [s.strip() for s in os.environ.get("KALSHI_LIFECYCLE_SERIES", "KXBTCD,KXETHD").split(",") if s.strip()]
SIGN_PATH = "/trade-api/ws/v2"


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


def auth_headers(key) -> dict[str, str]:
    ts = str(int(time.time() * 1000))
    return {
        "KALSHI-ACCESS-KEY": API_KEY_ID,
        "KALSHI-ACCESS-SIGNATURE": sign(key, ts, "GET", SIGN_PATH),
        "KALSHI-ACCESS-TIMESTAMP": ts,
    }


def series_prefix(ticker: str | None) -> str:
    if not ticker:
        return "<no-ticker>"
    for s in SERIES:
        if ticker.startswith(s + "-"):
            return s
    return "other"


async def run() -> int:
    if API_KEY_ID.startswith("<"):
        print("ERROR: missing keyid file or KALSHI_API_KEY_ID env", file=sys.stderr)
        return 2

    key = load_key()
    headers = auth_headers(key)

    print(f"Connecting to {WS_URL} (probe duration: {DURATION_S}s, series rollup: {SERIES})")
    async with websockets.connect(WS_URL, additional_headers=headers, max_size=2**21) as ws:
        sub = {
            "id": 1,
            "cmd": "subscribe",
            "params": {"channels": ["market_lifecycle_v2"]},
        }
        await ws.send(json.dumps(sub))
        print("Subscribe sent. Waiting for ack + frames...")

        type_counts: dict[tuple[str, str], int] = defaultdict(int)
        per_event_keys: dict[str, set[str]] = defaultdict(set)
        per_event_sample: dict[str, dict[str, Any]] = {}
        per_event_series: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
        ack_frames: list[dict[str, Any]] = []
        error_frames: list[dict[str, Any]] = []
        first_frame_at: float | None = None
        last_frame_at: float | None = None
        total_frames = 0

        deadline = time.monotonic() + DURATION_S
        next_status = time.monotonic() + 30
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=min(remaining, 5))
            except asyncio.TimeoutError:
                if time.monotonic() >= next_status:
                    print(f"  ... {total_frames} frames so far, {int(deadline - time.monotonic())}s left")
                    next_status = time.monotonic() + 30
                continue

            now = time.time()
            if first_frame_at is None:
                first_frame_at = now
            last_frame_at = now
            total_frames += 1

            try:
                fr = json.loads(raw)
            except json.JSONDecodeError:
                continue

            ftype = fr.get("type", "")
            if ftype == "ok":
                ack_frames.append(fr)
                continue
            if ftype == "error":
                error_frames.append(fr)
                print(f"  WS error frame: {fr}", file=sys.stderr)
                continue

            msg = fr.get("msg") or {}
            etype = msg.get("event_type", "<no-event-type>") if isinstance(msg, dict) else "<non-dict-msg>"
            type_counts[(ftype, etype)] += 1

            if isinstance(msg, dict):
                per_event_keys[etype].update(msg.keys())
                if etype not in per_event_sample:
                    per_event_sample[etype] = msg
                ticker = msg.get("market_ticker") or msg.get("ticker") or msg.get("event_ticker")
                per_event_series[etype][series_prefix(ticker)] += 1

            if time.monotonic() >= next_status:
                print(f"  ... {total_frames} frames so far, {int(deadline - time.monotonic())}s left")
                next_status = time.monotonic() + 30

    out = {
        "ws_url": WS_URL,
        "duration_s": DURATION_S,
        "series_rollup": SERIES,
        "first_frame_at": first_frame_at,
        "last_frame_at": last_frame_at,
        "total_frames": total_frames,
        "ack_frames": ack_frames,
        "error_frames": error_frames,
        "type_event_counts": {
            f"{ft}|{et}": n for (ft, et), n in sorted(type_counts.items(), key=lambda x: -x[1])
        },
        "per_event_keys": {et: sorted(keys) for et, keys in per_event_keys.items()},
        "per_event_sample": per_event_sample,
        "per_event_series": {
            et: dict(sorted(by_series.items(), key=lambda x: -x[1]))
            for et, by_series in per_event_series.items()
        },
    }

    out_dir = "tmp/probes"
    os.makedirs(out_dir, exist_ok=True)
    stamp = time.strftime("%Y-%m-%dT%H-%M", time.gmtime())
    out_path = os.path.join(out_dir, f"probe-lifecycle-{stamp}.json")
    with open(out_path, "w") as f:
        json.dump(out, f, indent=2, sort_keys=False)

    print()
    print(f"=== Captured {total_frames} frames over ~{DURATION_S}s; written to {out_path} ===")
    print()
    print("Per (frame.type, msg.event_type) counts (top):")
    for k, v in list(out["type_event_counts"].items())[:20]:
        print(f"  {v:6d}  {k}")
    print()
    print("Per event_type → keys observed in msg:")
    for et, keys in out["per_event_keys"].items():
        print(f"  {et}: {keys}")
    print()
    print("Per event_type → series rollup:")
    for et, by_series in out["per_event_series"].items():
        print(f"  {et}: {by_series}")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(run()))
