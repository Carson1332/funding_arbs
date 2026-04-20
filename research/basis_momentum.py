"""
Basis Momentum Signal.

Tracks the perp-spot basis (premium/discount) and its momentum.
A widening positive basis often precedes elevated funding rates,
providing an early entry signal.

Signal logic:
    basis = (perp_price - spot_price) / spot_price
    basis_momentum = basis.diff(lookback)

    basis_momentum > threshold  →  basis expanding, funding likely to rise
    basis_momentum < -threshold →  basis contracting, consider exit
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import numpy as np
import pandas as pd


@dataclass
class BasisMomentumParams:
    """Parameters for basis momentum signal."""

    lookback_periods: int = 12  # 12 × 8h = 4 days
    momentum_lookback: int = 6  # 6 × 8h = 2 days for momentum
    entry_threshold_bps: float = 5.0  # basis momentum > 5 bps
    exit_threshold_bps: float = -2.0  # basis momentum < -2 bps
    ema_span: int = 6  # EMA smoothing for basis


class BasisMomentum:
    """
    Compute basis momentum signals from spot/perp price data.
    """

    def __init__(self, params: Optional[BasisMomentumParams] = None):
        self.params = params or BasisMomentumParams()

    def compute(self, basis_df: pd.DataFrame) -> pd.DataFrame:
        """
        Compute basis momentum signal.

        Parameters
        ----------
        basis_df : pd.DataFrame
            Must have columns: timestamp, symbol, exchange, basis_bps
            (output from OHLCVDownloader.compute_basis)

        Returns
        -------
        pd.DataFrame with additional columns:
            basis_ema, basis_momentum, signal
        """
        df = basis_df.copy()
        df = df.sort_values(["symbol", "exchange", "timestamp"])

        results = []
        for (symbol, exchange), group in df.groupby(["symbol", "exchange"]):
            g = group.copy()

            # Smooth basis with EMA
            g["basis_ema"] = g["basis_bps"].ewm(span=self.params.ema_span).mean()

            # Basis momentum: change in smoothed basis over lookback
            g["basis_momentum"] = g["basis_ema"].diff(self.params.momentum_lookback)

            # Rolling basis stats
            g["basis_mean"] = g["basis_bps"].rolling(
                self.params.lookback_periods, min_periods=5
            ).mean()
            g["basis_std"] = g["basis_bps"].rolling(
                self.params.lookback_periods, min_periods=5
            ).std()

            # Signal generation
            g["signal"] = 0
            g.loc[g["basis_momentum"] > self.params.entry_threshold_bps, "signal"] = 1
            g.loc[g["basis_momentum"] < self.params.exit_threshold_bps, "signal"] = -1

            results.append(g)

        if not results:
            return pd.DataFrame()
        return pd.concat(results, ignore_index=True)

    def basis_regime(self, basis_df: pd.DataFrame) -> pd.DataFrame:
        """
        Classify basis into regimes: contango, backwardation, neutral.

        Useful for understanding market structure.
        """
        df = basis_df.copy()
        df["regime"] = "neutral"
        df.loc[df["basis_bps"] > 10, "regime"] = "contango"
        df.loc[df["basis_bps"] < -10, "regime"] = "backwardation"
        return df

    def basis_funding_correlation(
        self, basis_df: pd.DataFrame, funding_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Compute rolling correlation between basis and funding rate.

        High correlation validates the predictive power of basis momentum.
        """
        # Merge on timestamp + symbol + exchange
        merged = pd.merge(
            basis_df[["timestamp", "symbol", "exchange", "basis_bps"]],
            funding_df[["timestamp", "symbol", "exchange", "funding_rate"]],
            on=["timestamp", "symbol", "exchange"],
            how="inner",
        )
        merged = merged.sort_values(["symbol", "exchange", "timestamp"])

        results = []
        for (symbol, exchange), group in merged.groupby(["symbol", "exchange"]):
            g = group.copy()
            g["rolling_corr"] = (
                g["basis_bps"]
                .rolling(self.params.lookback_periods, min_periods=10)
                .corr(g["funding_rate"])
            )
            results.append(g)

        if not results:
            return pd.DataFrame()
        return pd.concat(results, ignore_index=True)
