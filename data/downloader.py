"""
Historical funding rate downloader.

Fetches 2+ years of 8-hour funding rate snapshots across multiple exchanges
(Binance, Bybit, OKX) using ccxt. Supports CoinAPI as a premium data source.
All data is cached locally as Parquet files in data/cache/ (gitignored).

Usage:
    python -m data.downloader --config config/default.yaml
"""

from __future__ import annotations

import argparse
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import ccxt
import pandas as pd
import yaml

from data.schemas import FundingRateRecord

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
CACHE_DIR = Path(__file__).resolve().parent / "cache" / "funding_rates"
EXCHANGES = ["binance", "bybit", "okx"]
RATE_LIMIT_SLEEP = 0.5  # seconds between API calls


def _ensure_cache_dir() -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)


def _cache_path(exchange: str, symbol: str) -> Path:
    """Return the Parquet cache path for a given exchange/symbol pair."""
    safe_symbol = symbol.replace("/", "_").replace(":", "_")
    return CACHE_DIR / f"{exchange}_{safe_symbol}.parquet"


# ---------------------------------------------------------------------------
# CoinAPI data source (premium)
# ---------------------------------------------------------------------------
def fetch_funding_coinapi(
    symbol_id: str,
    since: datetime,
    api_key: Optional[str] = None,
    limit: int = 10000,
) -> pd.DataFrame:
    """
    Fetch funding rate history from CoinAPI REST API.

    Parameters
    ----------
    symbol_id : str
        CoinAPI symbol ID, e.g. "BINANCE_PERP_BTC_USDT"
    since : datetime
        Start time (UTC).
    api_key : str, optional
        CoinAPI key. Falls back to COINAPI_KEY env var.
    limit : int
        Max records per request.

    Returns
    -------
    pd.DataFrame with columns [timestamp, symbol, exchange, funding_rate]
    """
    import requests

    key = api_key or os.environ.get("COINAPI_KEY", "")
    if not key:
        raise ValueError("CoinAPI key not provided. Set COINAPI_KEY env var or pass api_key.")

    url = f"https://rest.coinapi.io/v1/funding-rate/{symbol_id}/history"
    params = {
        "time_start": since.strftime("%Y-%m-%dT%H:%M:%S"),
        "limit": limit,
    }
    headers = {"X-CoinAPI-Key": key}
    resp = requests.get(url, params=params, headers=headers, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    if not data:
        return pd.DataFrame(columns=["timestamp", "symbol", "exchange", "funding_rate"])

    df = pd.DataFrame(data)
    # CoinAPI returns time_coinapi, time_exchange, rate, etc.
    df["timestamp"] = pd.to_datetime(df.get("time_exchange", df.get("time_coinapi")))
    df["funding_rate"] = df["rate"]
    df["symbol"] = symbol_id
    df["exchange"] = symbol_id.split("_")[0].lower() if "_" in symbol_id else "unknown"
    return df[["timestamp", "symbol", "exchange", "funding_rate"]]


# ---------------------------------------------------------------------------
# ccxt data source (free)
# ---------------------------------------------------------------------------
def _init_exchange(name: str) -> ccxt.Exchange:
    """Initialise a ccxt exchange instance with default options."""
    exchange_class = getattr(ccxt, name)
    exchange = exchange_class(
        {
            "enableRateLimit": True,
            "options": {"defaultType": "swap"},
        }
    )
    exchange.load_markets()
    return exchange


def fetch_funding_ccxt(
    exchange: ccxt.Exchange,
    exchange_name: str,
    symbol: str,
    since: datetime,
    limit: int = 1000,
) -> pd.DataFrame:
    """
    Fetch funding rate history for a single symbol via ccxt.

    Paginates automatically until all records since `since` are retrieved.
    """
    all_records: list[dict] = []
    since_ms = int(since.timestamp() * 1000)

    while True:
        try:
            raw = exchange.fetch_funding_rate_history(symbol, since=since_ms, limit=limit)
        except Exception as e:
            print(f"  [WARN] {exchange_name}/{symbol} fetch error: {e}")
            break

        if not raw:
            break

        for r in raw:
            all_records.append(
                {
                    "timestamp": pd.to_datetime(r["timestamp"], unit="ms", utc=True),
                    "symbol": symbol,
                    "exchange": exchange_name,
                    "funding_rate": r.get("fundingRate", 0.0),
                }
            )

        # Advance pagination cursor
        last_ts = raw[-1]["timestamp"]
        if last_ts <= since_ms:
            break
        since_ms = last_ts + 1

        if len(raw) < limit:
            break

        time.sleep(RATE_LIMIT_SLEEP)

    if not all_records:
        return pd.DataFrame(columns=["timestamp", "symbol", "exchange", "funding_rate"])

    return pd.DataFrame(all_records)


# ---------------------------------------------------------------------------
# Main downloader class
# ---------------------------------------------------------------------------
class FundingRateDownloader:
    """
    Multi-exchange funding rate downloader with local Parquet caching.

    Data is stored in data/cache/funding_rates/ and is .gitignored so that
    public users see the logic and results but not the raw data.
    """

    def __init__(self, config: dict):
        self.pairs: list[str] = config.get("universe", ["BTC/USDT:USDT"])
        self.exchange_names: list[str] = config.get("exchanges", EXCHANGES)
        self.coinapi_key: Optional[str] = config.get(
            "coinapi_key", os.environ.get("COINAPI_KEY")
        )
        self.use_coinapi: bool = config.get("use_coinapi", False)
        self.exchanges: dict[str, ccxt.Exchange] = {}
        _ensure_cache_dir()

    def _get_exchange(self, name: str) -> ccxt.Exchange:
        if name not in self.exchanges:
            print(f"  Initialising {name}...")
            self.exchanges[name] = _init_exchange(name)
        return self.exchanges[name]

    def _load_cache(self, exchange_name: str, symbol: str) -> Optional[pd.DataFrame]:
        """Load cached Parquet if it exists."""
        path = _cache_path(exchange_name, symbol)
        if path.exists():
            df = pd.read_parquet(path)
            return df
        return None

    def _save_cache(self, df: pd.DataFrame, exchange_name: str, symbol: str) -> None:
        """Save / append to Parquet cache."""
        path = _cache_path(exchange_name, symbol)
        if path.exists():
            existing = pd.read_parquet(path)
            df = pd.concat([existing, df], ignore_index=True)
            df = df.drop_duplicates(subset=["timestamp", "symbol", "exchange"])
            df = df.sort_values("timestamp").reset_index(drop=True)
        df.to_parquet(path, index=False)

    def fetch_symbol(
        self, exchange_name: str, symbol: str, since: datetime
    ) -> pd.DataFrame:
        """
        Fetch funding rates for one exchange/symbol pair.
        Uses cache to avoid re-downloading existing data.
        """
        cached = self._load_cache(exchange_name, symbol)
        effective_since = since

        if cached is not None and len(cached) > 0:
            last_cached = pd.to_datetime(cached["timestamp"]).max()
            if last_cached.tzinfo is None:
                last_cached = last_cached.tz_localize("UTC")
            effective_since = last_cached + timedelta(seconds=1)
            print(
                f"  Cache hit for {exchange_name}/{symbol}: "
                f"{len(cached)} records, fetching from {effective_since}"
            )

        # Fetch new data
        if self.use_coinapi and self.coinapi_key:
            coinapi_symbol = self._to_coinapi_symbol(exchange_name, symbol)
            new_data = fetch_funding_coinapi(
                coinapi_symbol, effective_since, api_key=self.coinapi_key
            )
        else:
            exc = self._get_exchange(exchange_name)
            new_data = fetch_funding_ccxt(exc, exchange_name, symbol, effective_since)

        if len(new_data) > 0:
            self._save_cache(new_data, exchange_name, symbol)
            print(f"  Fetched {len(new_data)} new records for {exchange_name}/{symbol}")

        # Return full dataset
        full = self._load_cache(exchange_name, symbol)
        return full if full is not None else pd.DataFrame()

    def fetch_all(self, since: datetime) -> pd.DataFrame:
        """Fetch full universe across all exchanges. Returns combined DataFrame."""
        frames: list[pd.DataFrame] = []
        total = len(self.exchange_names) * len(self.pairs)
        done = 0

        for exc_name in self.exchange_names:
            for pair in self.pairs:
                done += 1
                print(f"[{done}/{total}] {exc_name}/{pair}")
                try:
                    df = self.fetch_symbol(exc_name, pair, since)
                    if len(df) > 0:
                        frames.append(df)
                except Exception as e:
                    print(f"  [SKIP] {exc_name}/{pair}: {e}")

        if not frames:
            return pd.DataFrame(
                columns=["timestamp", "symbol", "exchange", "funding_rate"]
            )

        combined = pd.concat(frames, ignore_index=True)
        combined["funding_rate_annualised"] = combined["funding_rate"] * 3 * 365
        return combined

    @staticmethod
    def _to_coinapi_symbol(exchange: str, symbol: str) -> str:
        """Convert ccxt symbol to CoinAPI symbol ID."""
        # BTC/USDT:USDT -> BINANCE_PERP_BTC_USDT
        base_quote = symbol.split(":")[0]  # BTC/USDT
        base, quote = base_quote.split("/")
        return f"{exchange.upper()}_PERP_{base}_{quote}"

    @staticmethod
    def load_cached_data(
        exchange: Optional[str] = None, symbol: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Load all cached funding rate data (or filter by exchange/symbol).
        Useful for notebooks and research modules.
        """
        _ensure_cache_dir()
        frames = []
        for f in CACHE_DIR.glob("*.parquet"):
            df = pd.read_parquet(f)
            if exchange and not f.stem.startswith(exchange):
                continue
            if symbol:
                safe = symbol.replace("/", "_").replace(":", "_")
                if safe not in f.stem:
                    continue
            frames.append(df)

        if not frames:
            return pd.DataFrame(
                columns=["timestamp", "symbol", "exchange", "funding_rate"]
            )
        return pd.concat(frames, ignore_index=True).sort_values("timestamp").reset_index(drop=True)


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Download historical funding rates")
    parser.add_argument(
        "--config",
        type=str,
        default="config/default.yaml",
        help="Path to config YAML file",
    )
    parser.add_argument(
        "--since",
        type=str,
        default=None,
        help="Start date (YYYY-MM-DD). Defaults to config value.",
    )
    args = parser.parse_args()

    with open(args.config) as f:
        config = yaml.safe_load(f)

    # Also load universe.yaml if it exists next to the config file
    config_dir = Path(args.config).resolve().parent
    universe_path = config_dir / "universe.yaml"
    if universe_path.exists():
        with open(universe_path) as f:
            universe_config = yaml.safe_load(f)
            if universe_config and "universe" in universe_config:
                config["universe"] = universe_config["universe"]

    # Merge CoinAPI key from env if not in config
    if "coinapi_key" not in config:
        config["coinapi_key"] = os.environ.get("COINAPI_KEY")

    since_str = args.since or config.get("data_start", "2024-01-01")
    since = datetime.strptime(since_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)

    print(f"Downloading funding rates since {since.date()} ...")
    print(f"Exchanges: {config.get('exchanges', EXCHANGES)}")
    print(f"Universe: {len(config.get('universe', []))} pairs")
    print(f"CoinAPI: {'enabled' if config.get('use_coinapi') else 'disabled (using ccxt)'}")
    print()

    dl = FundingRateDownloader(config)
    df = dl.fetch_all(since)

    print(f"\nTotal records: {len(df)}")
    if len(df) > 0:
        print(f"Date range: {df['timestamp'].min()} -> {df['timestamp'].max()}")
        print(f"Exchanges: {df['exchange'].nunique()}")
        print(f"Symbols: {df['symbol'].nunique()}")


if __name__ == "__main__":
    main()
