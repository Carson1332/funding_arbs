"""
Database writer for funding rate data.

Supports PostgreSQL/TimescaleDB for production and SQLite for local development.
Connection string is read from DATABASE_URL env var or config.

Usage:
    from data.db import DataStore
    store = DataStore()
    store.write_funding_rates(df)
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

DEFAULT_SQLITE_PATH = Path(__file__).resolve().parent / "cache" / "funding_arb.db"


class DataStore:
    """
    Unified data store for all pipeline data.
    Uses PostgreSQL/TimescaleDB in production, SQLite locally.
    """

    def __init__(self, connection_url: Optional[str] = None):
        self.url = connection_url or os.environ.get(
            "DATABASE_URL", f"sqlite:///{DEFAULT_SQLITE_PATH}"
        )
        self.engine: Engine = create_engine(self.url)
        self._init_tables()

    def _init_tables(self) -> None:
        """Create tables if they don't exist."""
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    """
                CREATE TABLE IF NOT EXISTS funding_rates (
                    timestamp TIMESTAMP,
                    symbol VARCHAR(50),
                    exchange VARCHAR(20),
                    funding_rate DOUBLE PRECISION,
                    funding_rate_annualised DOUBLE PRECISION,
                    PRIMARY KEY (timestamp, symbol, exchange)
                )
            """
                )
            )
            conn.execute(
                text(
                    """
                CREATE TABLE IF NOT EXISTS ohlcv (
                    timestamp TIMESTAMP,
                    symbol VARCHAR(50),
                    exchange VARCHAR(20),
                    market_type VARCHAR(10),
                    open DOUBLE PRECISION,
                    high DOUBLE PRECISION,
                    low DOUBLE PRECISION,
                    close DOUBLE PRECISION,
                    volume DOUBLE PRECISION,
                    PRIMARY KEY (timestamp, symbol, exchange, market_type)
                )
            """
                )
            )
            conn.execute(
                text(
                    """
                CREATE TABLE IF NOT EXISTS open_interest (
                    timestamp TIMESTAMP,
                    symbol VARCHAR(50),
                    exchange VARCHAR(20),
                    open_interest DOUBLE PRECISION,
                    open_interest_usd DOUBLE PRECISION,
                    PRIMARY KEY (timestamp, symbol, exchange)
                )
            """
                )
            )

    def write_funding_rates(self, df: pd.DataFrame) -> int:
        """Write funding rate DataFrame to DB. Returns rows written."""
        if df.empty:
            return 0
        df.to_sql("funding_rates", self.engine, if_exists="append", index=False, method="multi")
        return len(df)

    def write_ohlcv(self, df: pd.DataFrame, market_type: str = "perp") -> int:
        """Write OHLCV DataFrame to DB."""
        if df.empty:
            return 0
        df = df.copy()
        df["market_type"] = market_type
        df.to_sql("ohlcv", self.engine, if_exists="append", index=False, method="multi")
        return len(df)

    def write_open_interest(self, df: pd.DataFrame) -> int:
        """Write OI DataFrame to DB."""
        if df.empty:
            return 0
        df.to_sql("open_interest", self.engine, if_exists="append", index=False, method="multi")
        return len(df)

    def read_funding_rates(
        self,
        symbol: Optional[str] = None,
        exchange: Optional[str] = None,
        since: Optional[str] = None,
    ) -> pd.DataFrame:
        """Read funding rates from DB with optional filters."""
        query = "SELECT * FROM funding_rates WHERE 1=1"
        params = {}
        if symbol:
            query += " AND symbol = :symbol"
            params["symbol"] = symbol
        if exchange:
            query += " AND exchange = :exchange"
            params["exchange"] = exchange
        if since:
            query += " AND timestamp >= :since"
            params["since"] = since
        query += " ORDER BY timestamp"
        return pd.read_sql(text(query), self.engine, params=params)

    def read_ohlcv(
        self,
        symbol: Optional[str] = None,
        exchange: Optional[str] = None,
        market_type: Optional[str] = None,
    ) -> pd.DataFrame:
        query = "SELECT * FROM ohlcv WHERE 1=1"
        params = {}
        if symbol:
            query += " AND symbol = :symbol"
            params["symbol"] = symbol
        if exchange:
            query += " AND exchange = :exchange"
            params["exchange"] = exchange
        if market_type:
            query += " AND market_type = :market_type"
            params["market_type"] = market_type
        query += " ORDER BY timestamp"
        return pd.read_sql(text(query), self.engine, params=params)
