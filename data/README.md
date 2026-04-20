# Data Layer

This module handles all data ingestion for the funding rate arbitrage pipeline.

## Modules

| Module | Description |
|--------|-------------|
| `downloader.py` | Historical funding rate pipeline (Binance, Bybit, OKX) via ccxt + CoinAPI |
| `spot_prices.py` | Spot & perpetual OHLCV for basis calculation |
| `oi_fetcher.py` | Open interest history from exchange public APIs |
| `db.py` | Database writer (PostgreSQL/TimescaleDB or SQLite fallback) |
| `schemas.py` | Pydantic models for all data types |

## Bootstrap Data

```bash
# 1. Set CoinAPI key (optional, for premium data)
export COINAPI_KEY="your_key_here"

# 2. Download funding rates (uses ccxt by default)
python -m data.downloader --config config/default.yaml

# 3. Download spot/perp OHLCV for basis
python -m data.spot_prices --config config/default.yaml

# 4. Download open interest
python -m data.oi_fetcher --config config/default.yaml
```

## Local Caching

All downloaded data is cached as Parquet files in `data/cache/`. This directory is
`.gitignore`'d so that public users can see the logic and results but not the raw data.

Subsequent runs only fetch new data since the last cached timestamp, avoiding
redundant API calls.

## Data Sources

- **ccxt** (free): Funding rates, OHLCV via exchange public APIs
- **CoinAPI** (premium): Consistent, timestamped data across all derivatives exchanges
- **Exchange REST APIs**: Open interest (Binance Futures, Bybit V5, OKX V5)
