"""Pydantic models for all data types in the funding arb pipeline."""

from __future__ import annotations

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field


class FundingRateRecord(BaseModel):
    """A single funding rate observation."""

    timestamp: datetime
    symbol: str
    exchange: str
    funding_rate: float = Field(..., description="Funding rate as a decimal (e.g., 0.0001 = 0.01%)")
    funding_rate_annualised: Optional[float] = Field(
        None, description="Annualised funding rate (rate * 3 * 365)"
    )

    class Config:
        json_schema_extra = {
            "example": {
                "timestamp": "2025-01-01T00:00:00Z",
                "symbol": "BTC/USDT:USDT",
                "exchange": "binance",
                "funding_rate": 0.0001,
                "funding_rate_annualised": 0.1095,
            }
        }


class OHLCVRecord(BaseModel):
    """A single OHLCV candle."""

    timestamp: datetime
    symbol: str
    exchange: str
    open: float
    high: float
    low: float
    close: float
    volume: float


class OpenInterestRecord(BaseModel):
    """A single open interest observation."""

    timestamp: datetime
    symbol: str
    exchange: str
    open_interest: float = Field(..., description="Open interest in base currency units")
    open_interest_usd: Optional[float] = Field(None, description="Open interest in USD notional")


class SpotPerpSpread(BaseModel):
    """Spot vs perpetual price spread (basis)."""

    timestamp: datetime
    symbol: str
    exchange: str
    spot_price: float
    perp_price: float
    basis: float = Field(..., description="(perp - spot) / spot as decimal")
    basis_bps: float = Field(..., description="Basis in basis points")


class BacktestMetrics(BaseModel):
    """Summary metrics from a backtest run."""

    sharpe: float
    annual_return_pct: float
    max_drawdown_pct: float
    sortino: float
    calmar: float
    total_trades: int
    backtest_start: str
    backtest_end: str
    win_rate: Optional[float] = None
    avg_funding_collected_daily: Optional[float] = None
