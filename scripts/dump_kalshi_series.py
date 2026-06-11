#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["cryptography>=42"]
# ///
"""
Enumerate every series on Kalshi prod and dump a CSV catalog.

Reads creds from ~/.config/kalshiflow/{kalshi.prod.keyid,kalshi.prod.pem},
walks /trade-api/v2/series with cursor paging, and writes
docs/__ignore__series.csv (ticker, title, category, frequency, tags, contract_url).

Run via uv (single-file script with PEP 723 inline deps):
    uv run scripts/dump_kalshi_series.py

Override creds with KALSHI_API_KEY_ID / KALSHI_PEM_PATH env vars if needed.
"""
from __future__ import annotations

import base64
import csv
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric.padding import MGF1, PSS

CRED_DIR = Path(os.path.expanduser("~/.config/kalshiflow"))
BASE_URL = "https://api.elections.kalshi.com"
SERIES_PATH = "/trade-api/v2/series"
PAGE_LIMIT = 200
OUT_PATH = Path(__file__).resolve().parents[1] / "docs" / "__ignore__series.csv"


def load_keyid() -> str:
    env = os.environ.get("KALSHI_API_KEY_ID")
    if env:
        return env.strip()
    return (CRED_DIR / "kalshi.prod.keyid").read_text().strip()


def load_key():
    pem_path = Path(os.environ.get("KALSHI_PEM_PATH") or (CRED_DIR / "kalshi.prod.pem"))
    return serialization.load_pem_private_key(pem_path.read_bytes(), password=None)


def sign(key, ts: str, method: str, path: str) -> str:
    msg = (ts + method + path).encode("utf-8")
    sig = key.sign(
        msg,
        PSS(mgf=MGF1(hashes.SHA256()), salt_length=PSS.DIGEST_LENGTH),
        hashes.SHA256(),
    )
    return base64.b64encode(sig).decode("ascii")


def get(key, keyid: str, path: str) -> dict:
    ts = str(int(time.time() * 1000))
    sig_path = path.split("?", 1)[0]
    headers = {
        "KALSHI-ACCESS-KEY": keyid,
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


def fetch_all(key, keyid: str) -> list[dict]:
    rows: list[dict] = []
    cursor = ""
    page = 0
    while True:
        page += 1
        params: dict[str, str] = {"limit": str(PAGE_LIMIT)}
        if cursor:
            params["cursor"] = cursor
        body = get(key, keyid, f"{SERIES_PATH}?{urllib.parse.urlencode(params)}")
        batch = body.get("series") or []
        rows.extend(batch)
        cursor = body.get("cursor") or ""
        print(f"page {page}: +{len(batch)} (total {len(rows)}) cursor={'…' if cursor else '(end)'}", file=sys.stderr)
        if not cursor or not batch:
            break
    return rows


def write_csv(rows: list[dict], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    cols = ["ticker", "title", "category", "frequency", "tags", "contract_url"]
    with path.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(cols)
        for s in sorted(rows, key=lambda r: (r.get("category") or "", r.get("ticker") or "")):
            tags = s.get("tags") or []
            w.writerow([
                s.get("ticker", ""),
                s.get("title", ""),
                s.get("category", ""),
                s.get("frequency", ""),
                "|".join(tags) if isinstance(tags, list) else str(tags),
                s.get("contract_url", ""),
            ])


def main() -> int:
    keyid = load_keyid()
    key = load_key()
    rows = fetch_all(key, keyid)
    write_csv(rows, OUT_PATH)
    print(f"wrote {len(rows)} series → {OUT_PATH}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
