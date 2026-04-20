"""
Portfolio tracker for backtest simulation.

Tracks positions, equity, drawdown, and P&L attribution.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

import numpy as np
import pandas as pd


@dataclass
class Position:
    """A single delta-neutral position (long spot + short perp)."""

    symbol: str
    exchange: str
    entry_time: datetime

    # Spot side
    spot_qty: float = 0.0
    spot_entry_price: float = 0.0

    # Perp side (negative qty = short)
    perp_qty: float = 0.0
    perp_entry_price: float = 0.0

    # Hedge ratio
    hedge_ratio: float = 1.0

    # P&L tracking
    funding_collected: float = 0.0
    trading_costs: float = 0.0
    unrealised_pnl: float = 0.0

    # Entry basis
    entry_basis: float = 0.0  # perp_price at entry (for funding calc)

    @property
    def notional(self) -> float:
        """Notional value of the spot side."""
        return self.spot_qty * self.spot_entry_price

    @property
    def net_pnl(self) -> float:
        """Net P&L = funding collected - costs + unrealised."""
        return self.funding_collected - self.trading_costs + self.unrealised_pnl

    def mark_to_market(self, spot_price: float, perp_price: float) -> float:
        """Update unrealised P&L based on current prices."""
        spot_pnl = self.spot_qty * (spot_price - self.spot_entry_price)
        perp_pnl = self.perp_qty * (perp_price - self.perp_entry_price)
        self.unrealised_pnl = spot_pnl + perp_pnl
        return self.unrealised_pnl


@dataclass
class PortfolioState:
    """Current state of the portfolio."""

    cash: float = 100_000.0  # starting capital in USDT
    initial_capital: float = 100_000.0
    positions: dict = field(default_factory=dict)  # symbol -> Position
    max_positions: int = 10
    max_position_pct: float = 0.15  # max 15% of equity per position

    # History tracking
    equity_history: list = field(default_factory=list)
    trade_log: list = field(default_factory=list)

    @property
    def total_notional(self) -> float:
        """Total notional across all positions."""
        return sum(p.notional for p in self.positions.values())

    @property
    def equity(self) -> float:
        """Current equity = cash + sum of position P&L."""
        return self.cash + sum(p.net_pnl for p in self.positions.values())

    @property
    def drawdown(self) -> float:
        """Current drawdown from peak equity."""
        if not self.equity_history:
            return 0.0
        peak = max(e["equity"] for e in self.equity_history)
        return (peak - self.equity) / peak if peak > 0 else 0.0

    @property
    def position_count(self) -> int:
        return len(self.positions)

    def can_open_position(self) -> bool:
        """Check if we can open a new position."""
        return self.position_count < self.max_positions

    def position_size(self) -> float:
        """Compute position size based on current equity and limits."""
        max_notional = self.equity * self.max_position_pct
        return min(max_notional, self.cash * 0.5)  # use at most 50% of free cash

    def record_equity(self, timestamp: datetime) -> None:
        """Record equity snapshot for history."""
        self.equity_history.append(
            {
                "timestamp": timestamp,
                "equity": self.equity,
                "cash": self.cash,
                "positions": self.position_count,
                "total_notional": self.total_notional,
                "drawdown": self.drawdown,
            }
        )

    def log_trade(
        self,
        timestamp: datetime,
        symbol: str,
        action: str,
        details: Optional[dict] = None,
    ) -> None:
        """Log a trade for analysis."""
        self.trade_log.append(
            {
                "timestamp": timestamp,
                "symbol": symbol,
                "action": action,
                **(details or {}),
            }
        )

    def to_equity_df(self) -> pd.DataFrame:
        """Convert equity history to DataFrame."""
        if not self.equity_history:
            return pd.DataFrame()
        df = pd.DataFrame(self.equity_history)
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df = df.set_index("timestamp")
        return df

    def to_returns(self) -> pd.Series:
        """Convert equity history to returns series."""
        eq = self.to_equity_df()
        if eq.empty:
            return pd.Series(dtype=float)
        returns = eq["equity"].pct_change().dropna()
        returns.name = "returns"
        return returns

    def to_trade_log_df(self) -> pd.DataFrame:
        """Convert trade log to DataFrame."""
        if not self.trade_log:
            return pd.DataFrame()
        return pd.DataFrame(self.trade_log)
