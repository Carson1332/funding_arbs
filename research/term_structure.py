"""
Funding Rate Term Structure Analysis.

Analyses the "term structure" of funding rates by looking at how
funding rates vary across different rolling windows (short-term vs long-term).
This is analogous to yield curve analysis in fixed income.

Key concepts:
    - Short-term rate: latest 8h funding rate
    - Medium-term rate: 7-day rolling average
    - Long-term rate: 30-day rolling average
    - Term structure slope: short_term - long_term
    - Inversion: when short-term < long-term (bearish signal)
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import pandas as pd


@dataclass
class TermStructureParams:
    """Parameters for term structure analysis."""

    short_window: int = 3  # 3 × 8h = 1 day
    medium_window: int = 21  # 21 × 8h = 7 days
    long_window: int = 90  # 90 × 8h = 30 days
    inversion_threshold: float = -0.0001  # slope below this = inverted


class FundingTermStructure:
    """
    Analyse funding rate term structure across multiple time horizons.
    """

    def __init__(self, params: Optional[TermStructureParams] = None):
        self.params = params or TermStructureParams()

    def compute(self, funding_df: pd.DataFrame) -> pd.DataFrame:
        """
        Compute term structure metrics for each symbol/exchange.

        Parameters
        ----------
        funding_df : pd.DataFrame
            Must have columns: timestamp, symbol, exchange, funding_rate

        Returns
        -------
        pd.DataFrame with additional columns:
            rate_short, rate_medium, rate_long, slope, curvature, regime
        """
        df = funding_df.copy()
        df = df.sort_values(["symbol", "exchange", "timestamp"])

        results = []
        for (symbol, exchange), group in df.groupby(["symbol", "exchange"]):
            g = group.copy()

            # Rolling averages at different horizons
            g["rate_short"] = g["funding_rate"].rolling(
                self.params.short_window, min_periods=1
            ).mean()
            g["rate_medium"] = g["funding_rate"].rolling(
                self.params.medium_window, min_periods=5
            ).mean()
            g["rate_long"] = g["funding_rate"].rolling(
                self.params.long_window, min_periods=10
            ).mean()

            # Annualised versions
            g["rate_short_ann"] = g["rate_short"] * 3 * 365
            g["rate_medium_ann"] = g["rate_medium"] * 3 * 365
            g["rate_long_ann"] = g["rate_long"] * 3 * 365

            # Term structure slope: short - long
            g["slope"] = g["rate_short"] - g["rate_long"]

            # Curvature: 2 * medium - short - long (butterfly)
            g["curvature"] = 2 * g["rate_medium"] - g["rate_short"] - g["rate_long"]

            # Regime classification
            g["regime"] = "normal"
            g.loc[g["slope"] < self.params.inversion_threshold, "regime"] = "inverted"
            g.loc[g["slope"] > abs(self.params.inversion_threshold) * 3, "regime"] = "steep"

            # Slope momentum
            g["slope_momentum"] = g["slope"].diff(self.params.short_window)

            results.append(g)

        if not results:
            return pd.DataFrame()
        return pd.concat(results, ignore_index=True)

    def cross_symbol_term_structure(self, funding_df: pd.DataFrame) -> pd.DataFrame:
        """
        Compare term structure across symbols at the latest timestamp.

        Useful for identifying which pairs have the most attractive
        funding rate term structure.
        """
        ts_df = self.compute(funding_df)
        if ts_df.empty:
            return pd.DataFrame()

        latest = (
            ts_df.sort_values("timestamp")
            .groupby(["symbol", "exchange"])
            .last()
            .reset_index()
        )

        # Rank by slope (steeper = more attractive for funding arb)
        latest["slope_rank"] = latest["slope"].rank(ascending=False)
        latest["rate_short_rank"] = latest["rate_short_ann"].rank(ascending=False)

        return latest.sort_values("slope", ascending=False).reset_index(drop=True)

    def regime_transitions(self, funding_df: pd.DataFrame) -> pd.DataFrame:
        """
        Detect regime transitions (normal → inverted, inverted → steep, etc.).

        Regime changes are important signals for position management.
        """
        ts_df = self.compute(funding_df)
        if ts_df.empty:
            return pd.DataFrame()

        results = []
        for (symbol, exchange), group in ts_df.groupby(["symbol", "exchange"]):
            g = group.copy()
            g["prev_regime"] = g["regime"].shift(1)
            transitions = g[g["regime"] != g["prev_regime"]].copy()
            if len(transitions) > 0:
                transitions["transition"] = (
                    transitions["prev_regime"] + " → " + transitions["regime"]
                )
                results.append(transitions)

        if not results:
            return pd.DataFrame()
        return pd.concat(results, ignore_index=True)
