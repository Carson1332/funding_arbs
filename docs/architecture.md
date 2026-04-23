# Technical Architecture

## System Overview

The funding-arb system is organised into four layers, each with a clear responsibility boundary. Data flows from left to right: exchange APIs produce raw market data, which is ingested and cached by the data layer, transformed into signals by the research layer, consumed by the backtest engine, and finally summarised in the report layer.

```
Exchange APIs → Data Layer → Research Layer → Backtest Engine → Report Layer
(ccxt/CoinAPI)   (cache)      (signals)        (simulation)      (metrics/tearsheet)
```

## Data Layer (`data/`)

The data layer is responsible for all external data ingestion and local caching. It supports three data types: funding rates, OHLCV prices (spot and perpetual), and open interest.

**Downloader** (`downloader.py`) is the primary entry point. It uses ccxt to fetch funding rate history from Binance, Bybit, and OKX, with an optional CoinAPI integration for premium data. All data is cached as Parquet files in `data/cache/`, which is `.gitignore`'d. Subsequent runs perform incremental updates — only fetching data newer than the latest cached timestamp.

**Spot Prices** (`spot_prices.py`) fetches OHLCV candles for both spot and perpetual markets, aligned to 8-hour intervals. It computes the spot-perp basis (premium/discount) which feeds into the basis momentum signal.

**OI Fetcher** (`oi_fetcher.py`) uses exchange-specific REST APIs to fetch open interest history. Each exchange has a different API format, so the module provides exchange-specific adapters behind a unified interface.

**Database** (`db.py`) provides an optional PostgreSQL/TimescaleDB backend for production deployments. For local development, it falls back to SQLite. The Parquet cache is the primary data store for the backtest; the database is used when running in Docker or production.

## Research Layer (`research/`)

Each research module is a standalone signal generator that takes a DataFrame of market data and returns an augmented DataFrame with signal columns. This design allows signals to be composed, tested independently, and swapped in/out without modifying the backtest engine.

The **FundingZScore** class computes rolling z-scores and generates entry/exit signals. The **BasisMomentum** class tracks the rate of change of the spot-perp spread. The **OIConcentration** class detects crowded trades and cross-exchange divergence. The **FundingTermStructure** class classifies the funding rate regime. The **DynamicHedgeRatio** class uses a Kalman filter to estimate time-varying hedge ratios.

## Backtest Engine (`backtest/`)

The backtest engine replays historical 8-hour funding snapshots chronologically. At each timestep, it performs the following operations in order: collect funding payments on existing positions, adjust hedge ratios if drift exceeds the threshold, check risk limits (drawdown, per-position loss), process entry/exit signals, mark positions to market, and record the equity snapshot.

The **FeeModel** class models all trading costs including spot and perpetual maker/taker fees, slippage, and funding payments. Fee schedules are pre-configured for Binance, Bybit, and OKX at VIP-0 tier rates (e.g., 50 bps round-trip for Realistic). It also deducts a 4% annual opportunity cost on deployed collateral.

The **PortfolioState** class tracks all positions, cash, equity history, and trade log. It enforces position limits and computes real-time drawdown. Crucially, its drawdown exits and equity-scaled sizing create path-dependent simulations across different fee scenarios.

## Report Layer

The **BacktestReport** class generates four output artifacts. The `metrics.json` file contains machine-readable metrics (Sharpe, return, drawdown, etc.) that are extracted by the CI workflow for badge updates. The `equity_curve.csv` file contains the full equity time series. The `tearsheet.html` file is a comprehensive QuantStats report. The `trade_log.csv` file contains every trade with entry/exit details and P&L attribution.

## CI/CD Pipeline

Two GitHub Actions workflows automate the development and reporting cycle. The **CI workflow** runs on every pull request, performing linting (ruff), type checking (mypy), and unit tests (pytest). The **Backtest workflow** runs weekly (Sunday 6am UTC) and on push to research/backtest/config paths. It downloads fresh data, runs the full backtest, extracts metrics for BYOB badge updates, commits the results, and publishes the tearsheet to GitHub Pages.

## Deployment Options

For local development, the system runs entirely on the filesystem with Parquet caching and SQLite. For production, `docker compose up` starts a PostgreSQL/TimescaleDB instance alongside the application. A Jupyter notebook server is also available via `docker compose up notebook` for interactive research.
