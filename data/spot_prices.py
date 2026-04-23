"""
Spot & Perpetual OHLCV price downloader.

Fetches spot and perpetual OHLCV candles for basis calculation.
Cached locally as Parquet in data/cache/ohlcv/ (gitignored).

Usage:
    python -m data.spot_prices --config config/default.yaml
"""

from __future__ import annotations

import argparse
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import ccxt
import pandas as pd
import yaml

CACHE_DIR = Path(__file__).resolve().parent / "cache" / "ohlcv"
RATE_LIMIT_SLEEP = 0.5


def _ensure_cache_dir() -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)


def _cache_path(exchange: str, symbol: str, market_type: str) -> Path:
    safe_symbol = symbol.replace("/", "_").replace(":", "_")
    return CACHE_DIR / f"{exchange}_{safe_symbol}_{market_type}.parquet"


def _init_exchange(name: str, market_type: str = "swap") -> ccxt.Exchange:
    exchange_class = getattr(ccxt, name)
    exchange = exchange_class(
        {
            "enableRateLimit": True,
            "options": {"defaultType": market_type},
        }
    )
    exchange.load_markets()
    return exchange


class OHLCVDownloader:
    """
    Downloads spot and perpetual OHLCV data for basis calculation.
    Uses 8h candles to align with funding rate intervals.
    """

    def __init__(self, config: dict):
        self.pairs: list[str] = config.get("universe", ["BTC/USDT:USDT"])
        self.exchange_names: list[str] = config.get("exchanges", ["binance", "bybit", "okx"])
        self.timeframe: str = config.get("ohlcv_timeframe", "8h")
        self.exchanges: dict[str, dict[str, ccxt.Exchange]] = {}
        _ensure_cache_dir()

    def _get_exchange(self, name: str, market_type: str) -> ccxt.Exchange:
        key = f"{name}_{market_type}"
        if key not in self.exchanges:
            self.exchanges[key] = _init_exchange(name, market_type)
        return self.exchanges[key]

    def _load_cache(self, exchange: str, symbol: str, market_type: str) -> Optional[pd.DataFrame]:
        path = _cache_path(exchange, symbol, market_type)
        if path.exists():
            return pd.read_parquet(path)
        return None

    def _save_cache(self, df: pd.DataFrame, exchange: str, symbol: str, market_type: str) -> None:
        path = _cache_path(exchange, symbol, market_type)
        if path.exists():
            existing = pd.read_parquet(path)
            df = pd.concat([existing, df], ignore_index=True)
            df = df.drop_duplicates(subset=["timestamp", "symbol", "exchange"])
            df = df.sort_values("timestamp").reset_index(drop=True)
        df.to_parquet(path, index=False)

    def _fetch_ohlcv(
        self,
        exchange_name: str,
        symbol: str,
        market_type: str,
        since: datetime,
        limit: int = 500,
    ) -> pd.DataFrame:
        """Fetch OHLCV candles with pagination."""
        exc = self._get_exchange(exchange_name, market_type)
        all_candles = []
        since_ms = int(since.timestamp() * 1000)

        while True:
            try:
                candles = exc.fetch_ohlcv(symbol, self.timeframe, since=since_ms, limit=limit)
            except Exception as e:
                print(f"  [WARN] {exchange_name}/{symbol}/{market_type}: {e}")
                break

            if not candles:
                break

            for c in candles:
                all_candles.append(
                    {
                        "timestamp": pd.to_datetime(c[0], unit="ms", utc=True),
                        "symbol": symbol,
                        "exchange": exchange_name,
                        "open": c[1],
                        "high": c[2],
                        "low": c[3],
                        "close": c[4],
                        "volume": c[5],
                    }
                )

            last_ts = candles[-1][0]
            if last_ts <= since_ms:
                break
            since_ms = last_ts + 1

            if len(candles) < limit:
                break

            time.sleep(RATE_LIMIT_SLEEP)

        if not all_candles:
            return pd.DataFrame(
                columns=[
                    "timestamp", "symbol", "exchange", "open", "high",
                    "low", "close", "volume",
                ]
            )
        return pd.DataFrame(all_candles)

    def fetch_spot_and_perp(
        self, exchange_name: str, symbol: str, since: datetime
    ) -> dict[str, pd.DataFrame]:
        """
        Fetch both spot and perpetual OHLCV for a symbol.

        Returns dict with keys 'spot' and 'perp', each a DataFrame.
        """
        # Derive spot symbol from perp symbol: BTC/USDT:USDT -> BTC/USDT
        spot_symbol = symbol.split(":")[0]

        results = {}
        for market_type, sym in [("spot", spot_symbol), ("swap", symbol)]:
            label = "spot" if market_type == "spot" else "perp"

            cached = self._load_cache(exchange_name, sym, label)
            effective_since = since

            if cached is not None and len(cached) > 0:
                last_ts = pd.to_datetime(cached["timestamp"]).max()
                if last_ts.tzinfo is None:
                    last_ts = last_ts.tz_localize("UTC")
                effective_since = last_ts + timedelta(seconds=1)

            new_data = self._fetch_ohlcv(exchange_name, sym, market_type, effective_since)
            if len(new_data) > 0:
                self._save_cache(new_data, exchange_name, sym, label)

            full = self._load_cache(exchange_name, sym, label)
            results[label] = full if full is not None else pd.DataFrame()

        return results

    def compute_basis(
        self, spot_df: pd.DataFrame, perp_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Compute spot-perp basis from aligned OHLCV data.

        Returns DataFrame with columns:
            timestamp, symbol, exchange, spot_close, perp_close, basis, basis_bps
        """
        if spot_df.empty or perp_df.empty:
            return pd.DataFrame()

        spot = spot_df[["timestamp", "close"]].rename(columns={"close": "spot_close"})
        perp = perp_df[["timestamp", "close"]].rename(columns={"close": "perp_close"})

        merged = pd.merge_asof(
            perp.sort_values("timestamp"),
            spot.sort_values("timestamp"),
            on="timestamp",
            direction="nearest",
            tolerance=pd.Timedelta("1h"),
        )
        merged = merged.dropna()
        merged["basis"] = (merged["perp_close"] - merged["spot_close"]) / merged["spot_close"]
        merged["basis_bps"] = merged["basis"] * 10000

        if len(perp_df) > 0:
            merged["symbol"] = perp_df["symbol"].iloc[0]
            merged["exchange"] = perp_df["exchange"].iloc[0]

        return merged

    def fetch_all(self, since: datetime) -> pd.DataFrame:
        """Fetch spot+perp OHLCV and compute basis for the full universe."""
        frames = []
        total = len(self.exchange_names) * len(self.pairs)
        done = 0

        for exc in self.exchange_names:
            for pair in self.pairs:
                done += 1
                print(f"[{done}/{total}] OHLCV {exc}/{pair}")
                try:
                    data = self.fetch_spot_and_perp(exc, pair, since)
                    basis = self.compute_basis(data["spot"], data["perp"])
                    if len(basis) > 0:
                        frames.append(basis)
                except Exception as e:
                    print(f"  [SKIP] {exc}/{pair}: {e}")

        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True)

    @staticmethod
    def load_cached_ohlcv(
        exchange: Optional[str] = None,
        symbol: Optional[str] = None,
        market_type: Optional[str] = None,
    ) -> pd.DataFrame:
        """Load cached OHLCV data for notebooks/research."""
        _ensure_cache_dir()
        frames = []
        for f in CACHE_DIR.glob("*.parquet"):
            if exchange and not f.stem.startswith(exchange):
                continue
            if symbol:
                safe = symbol.replace("/", "_").replace(":", "_")
                if safe not in f.stem:
                    continue
            if market_type and not f.stem.endswith(market_type):
                continue
            frames.append(pd.read_parquet(f))

        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True).sort_values("timestamp").reset_index(drop=True)


def main():
    parser = argparse.ArgumentParser(description="Download spot & perp OHLCV data")
    parser.add_argument("--config", type=str, default="config/default.yaml")
    parser.add_argument("--since", type=str, default=None)
    args = parser.parse_args()

    with open(args.config) as f:
        config = yaml.safe_load(f)

    since_str = args.since or config.get("data_start", "2024-01-01")
    since = datetime.strptime(since_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)

    dl = OHLCVDownloader(config)
    df = dl.fetch_all(since)
    print(f"\nTotal basis records: {len(df)}")


def load_all_basis_data() -> pd.DataFrame:
    """Load and compute spot-perp basis for all cached OHLCV pairs.

    Returns DataFrame with columns:
        timestamp, symbol, exchange, spot_close, perp_close, basis, basis_bps
    """
    cache_dir = Path(__file__).resolve().parent / "cache" / "ohlcv"
    if not cache_dir.exists():
        return pd.DataFrame()

    files = list(cache_dir.glob("*.parquet"))
    spot_files = [f for f in files if f.stem.endswith("_spot")]

    downloader = OHLCVDownloader({})
    basis_frames: list[pd.DataFrame] = []

    for spot_f in spot_files:
        perp_name = spot_f.stem.replace("_spot", "_perp") + ".parquet"
        perp_f = spot_f.with_name(perp_name)
        if not perp_f.exists():
            continue
        try:
            spot_df = pd.read_parquet(spot_f)
            perp_df = pd.read_parquet(perp_f)
            basis = downloader.compute_basis(spot_df, perp_df)
            if len(basis) > 0:
                basis_frames.append(basis)
        except Exception:
            continue

    if not basis_frames:
        return pd.DataFrame()
    return pd.concat(basis_frames, ignore_index=True)


if __name__ == "__main__":
    main()
