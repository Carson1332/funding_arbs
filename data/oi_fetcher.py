"""
Open Interest history downloader.

Fetches OI data from Binance, Bybit, OKX via their public REST APIs.
Cached locally as Parquet in data/cache/open_interest/ (gitignored).

Usage:
    python -m data.oi_fetcher --config config/default.yaml
"""

from __future__ import annotations

import argparse
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import pandas as pd
import requests
import yaml

CACHE_DIR = Path(__file__).resolve().parent / "cache" / "open_interest"
RATE_LIMIT_SLEEP = 0.5


def _ensure_cache_dir() -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)


def _cache_path(exchange: str, symbol: str) -> Path:
    safe_symbol = symbol.replace("/", "_").replace(":", "_")
    return CACHE_DIR / f"{exchange}_{safe_symbol}.parquet"


# ---------------------------------------------------------------------------
# Exchange-specific OI fetchers
# ---------------------------------------------------------------------------

def _fetch_oi_binance(symbol: str, since: datetime, limit: int = 500) -> pd.DataFrame:
    """Fetch OI history from Binance Futures API."""
    # Convert BTC/USDT:USDT -> BTCUSDT
    clean_symbol = symbol.split(":")[0].replace("/", "")
    url = "https://fapi.binance.com/futures/data/openInterestHist"

    records = []
    start_ms = int(since.timestamp() * 1000)

    while True:
        params = {
            "symbol": clean_symbol,
            "period": "4h",
            "startTime": start_ms,
            "limit": limit,
        }
        try:
            resp = requests.get(url, params=params, timeout=15)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            print(f"  [WARN] Binance OI {clean_symbol}: {e}")
            break

        if not data:
            break

        for d in data:
            records.append(
                {
                    "timestamp": pd.to_datetime(d["timestamp"], unit="ms", utc=True),
                    "symbol": symbol,
                    "exchange": "binance",
                    "open_interest": float(d.get("sumOpenInterest", 0)),
                    "open_interest_usd": float(d.get("sumOpenInterestValue", 0)),
                }
            )

        last_ts = data[-1]["timestamp"]
        if last_ts <= start_ms:
            break
        start_ms = last_ts + 1

        if len(data) < limit:
            break
        time.sleep(RATE_LIMIT_SLEEP)

    if not records:
        return pd.DataFrame(
            columns=["timestamp", "symbol", "exchange", "open_interest", "open_interest_usd"]
        )
    return pd.DataFrame(records)


def _fetch_oi_bybit(symbol: str, since: datetime, limit: int = 200) -> pd.DataFrame:
    """Fetch OI history from Bybit V5 API."""
    # Convert BTC/USDT:USDT -> BTCUSDT
    clean_symbol = symbol.split(":")[0].replace("/", "")
    url = "https://api.bybit.com/v5/market/open-interest"

    records = []
    start_ms = int(since.timestamp() * 1000)
    end_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

    params = {
        "category": "linear",
        "symbol": clean_symbol,
        "intervalTime": "4h",
        "startTime": start_ms,
        "endTime": end_ms,
        "limit": limit,
    }
    try:
        resp = requests.get(url, params=params, timeout=15)
        resp.raise_for_status()
        result = resp.json().get("result", {})
        data = result.get("list", [])
    except Exception as e:
        print(f"  [WARN] Bybit OI {clean_symbol}: {e}")
        return pd.DataFrame(
            columns=["timestamp", "symbol", "exchange", "open_interest", "open_interest_usd"]
        )

    for d in data:
        records.append(
            {
                "timestamp": pd.to_datetime(int(d["timestamp"]), unit="ms", utc=True),
                "symbol": symbol,
                "exchange": "bybit",
                "open_interest": float(d.get("openInterest", 0)),
                "open_interest_usd": None,
            }
        )

    if not records:
        return pd.DataFrame(
            columns=["timestamp", "symbol", "exchange", "open_interest", "open_interest_usd"]
        )
    return pd.DataFrame(records)


def _fetch_oi_okx(symbol: str, since: datetime) -> pd.DataFrame:
    """Fetch current OI snapshot from OKX (historical OI requires authenticated API)."""
    # Convert BTC/USDT:USDT -> BTC-USDT-SWAP
    base_quote = symbol.split(":")[0]
    base, quote = base_quote.split("/")
    inst_id = f"{base}-{quote}-SWAP"
    url = "https://www.okx.com/api/v5/public/open-interest"

    try:
        resp = requests.get(url, params={"instType": "SWAP", "instId": inst_id}, timeout=15)
        resp.raise_for_status()
        data = resp.json().get("data", [])
    except Exception as e:
        print(f"  [WARN] OKX OI {inst_id}: {e}")
        return pd.DataFrame(
            columns=["timestamp", "symbol", "exchange", "open_interest", "open_interest_usd"]
        )

    records = []
    for d in data:
        records.append(
            {
                "timestamp": pd.to_datetime(int(d["ts"]), unit="ms", utc=True),
                "symbol": symbol,
                "exchange": "okx",
                "open_interest": float(d.get("oi", 0)),
                "open_interest_usd": float(d.get("oiCcy", 0)) if d.get("oiCcy") else None,
            }
        )

    if not records:
        return pd.DataFrame(
            columns=["timestamp", "symbol", "exchange", "open_interest", "open_interest_usd"]
        )
    return pd.DataFrame(records)


# ---------------------------------------------------------------------------
# Main OI downloader
# ---------------------------------------------------------------------------

OI_FETCHERS = {
    "binance": _fetch_oi_binance,
    "bybit": _fetch_oi_bybit,
    "okx": _fetch_oi_okx,
}


class OpenInterestDownloader:
    """Multi-exchange open interest downloader with local caching."""

    def __init__(self, config: dict):
        self.pairs = config.get("universe", ["BTC/USDT:USDT"])
        self.exchange_names = config.get("exchanges", ["binance", "bybit", "okx"])
        _ensure_cache_dir()

    def _load_cache(self, exchange: str, symbol: str) -> Optional[pd.DataFrame]:
        path = _cache_path(exchange, symbol)
        if path.exists():
            return pd.read_parquet(path)
        return None

    def _save_cache(self, df: pd.DataFrame, exchange: str, symbol: str) -> None:
        path = _cache_path(exchange, symbol)
        if path.exists():
            existing = pd.read_parquet(path)
            df = pd.concat([existing, df], ignore_index=True)
            df = df.drop_duplicates(subset=["timestamp", "symbol", "exchange"])
            df = df.sort_values("timestamp").reset_index(drop=True)
        df.to_parquet(path, index=False)

    def fetch_all(self, since: datetime) -> pd.DataFrame:
        """Fetch OI for the full universe."""
        frames = []
        total = len(self.exchange_names) * len(self.pairs)
        done = 0

        for exc in self.exchange_names:
            fetcher = OI_FETCHERS.get(exc)
            if not fetcher:
                print(f"  [SKIP] No OI fetcher for {exc}")
                continue

            for pair in self.pairs:
                done += 1
                print(f"[{done}/{total}] OI {exc}/{pair}")
                try:
                    cached = self._load_cache(exc, pair)
                    effective_since = since
                    if cached is not None and len(cached) > 0:
                        last_ts = pd.to_datetime(cached["timestamp"]).max()
                        if last_ts.tzinfo is None:
                            last_ts = last_ts.tz_localize("UTC")
                        effective_since = last_ts + timedelta(seconds=1)

                    new_data = fetcher(pair, effective_since)
                    if len(new_data) > 0:
                        self._save_cache(new_data, exc, pair)
                        print(f"  Fetched {len(new_data)} OI records")

                    full = self._load_cache(exc, pair)
                    if full is not None and len(full) > 0:
                        frames.append(full)
                except Exception as e:
                    print(f"  [SKIP] {exc}/{pair}: {e}")

        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True)

    @staticmethod
    def load_cached_oi(
        exchange: Optional[str] = None, symbol: Optional[str] = None
    ) -> pd.DataFrame:
        """Load cached OI data for notebooks/research."""
        _ensure_cache_dir()
        frames = []
        for f in CACHE_DIR.glob("*.parquet"):
            if exchange and not f.stem.startswith(exchange):
                continue
            if symbol:
                safe = symbol.replace("/", "_").replace(":", "_")
                if safe not in f.stem:
                    continue
            frames.append(pd.read_parquet(f))
        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True).sort_values("timestamp").reset_index(drop=True)


def main():
    parser = argparse.ArgumentParser(description="Download open interest data")
    parser.add_argument("--config", type=str, default="config/default.yaml")
    parser.add_argument("--since", type=str, default=None)
    args = parser.parse_args()

    with open(args.config) as f:
        config = yaml.safe_load(f)

    since_str = args.since or config.get("data_start", "2024-01-01")
    since = datetime.strptime(since_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)

    dl = OpenInterestDownloader(config)
    df = dl.fetch_all(since)
    print(f"\nTotal OI records: {len(df)}")


if __name__ == "__main__":
    main()
