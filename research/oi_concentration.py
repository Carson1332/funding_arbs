"""
Open Interest Concentration Signal.

Monitors OI changes across exchanges to detect when speculative positioning
is building up. Rising OI + high funding = crowded trade risk.
Diverging OI across exchanges = potential cross-exchange opportunity.

Signal logic:
    oi_change = OI.pct_change(lookback)
    oi_zscore = zscore(oi_change, rolling_window)

    High OI growth + high funding → crowded, reduce position
    OI divergence across exchanges → cross-exchange arb opportunity
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import numpy as np
import pandas as pd


@dataclass
class OIConcentrationParams:
    """Parameters for OI concentration signal."""

    lookback_periods: int = 18  # 18 × 4h = 3 days
    zscore_window: int = 90  # rolling window for z-score
    crowded_threshold: float = 2.0  # z-score above which OI is "crowded"
    divergence_threshold: float = 0.10  # 10% OI growth divergence between exchanges


class OIConcentration:
    """
    Compute open interest concentration and divergence signals.
    """

    def __init__(self, params: Optional[OIConcentrationParams] = None):
        self.params = params or OIConcentrationParams()

    def compute(self, oi_df: pd.DataFrame) -> pd.DataFrame:
        """
        Compute OI concentration metrics.

        Parameters
        ----------
        oi_df : pd.DataFrame
            Must have columns: timestamp, symbol, exchange, open_interest

        Returns
        -------
        pd.DataFrame with additional columns:
            oi_pct_change, oi_zscore, crowded_flag
        """
        df = oi_df.copy()
        df = df.sort_values(["symbol", "exchange", "timestamp"])

        results = []
        for (symbol, exchange), group in df.groupby(["symbol", "exchange"]):
            g = group.copy()

            # OI percentage change
            g["oi_pct_change"] = g["open_interest"].pct_change(self.params.lookback_periods)

            # Rolling z-score of OI change
            rolling_mean = g["oi_pct_change"].rolling(
                self.params.zscore_window, min_periods=10
            ).mean()
            rolling_std = g["oi_pct_change"].rolling(
                self.params.zscore_window, min_periods=10
            ).std()
            g["oi_zscore"] = (g["oi_pct_change"] - rolling_mean) / rolling_std.replace(0, np.nan)

            # Crowded flag
            g["crowded_flag"] = (g["oi_zscore"] > self.params.crowded_threshold).astype(int)

            # OI level relative to recent history
            g["oi_percentile"] = g["open_interest"].rolling(
                self.params.zscore_window, min_periods=10
            ).apply(lambda x: pd.Series(x).rank(pct=True).iloc[-1], raw=False)

            results.append(g)

        if not results:
            return pd.DataFrame()
        return pd.concat(results, ignore_index=True)

    def cross_exchange_divergence(self, oi_df: pd.DataFrame) -> pd.DataFrame:
        """
        Detect OI divergence across exchanges for the same symbol.

        When OI grows significantly on one exchange but not another,
        it may indicate localised speculative activity.
        """
        df = oi_df.copy()
        df = df.sort_values("timestamp")

        # Compute OI growth per exchange
        growth_frames = []
        for (symbol, exchange), group in df.groupby(["symbol", "exchange"]):
            g = group.copy()
            g["oi_growth"] = g["open_interest"].pct_change(self.params.lookback_periods)
            growth_frames.append(g)

        if not growth_frames:
            return pd.DataFrame()

        growth_df = pd.concat(growth_frames, ignore_index=True)

        # Pivot to compare exchanges
        pivot = growth_df.pivot_table(
            index=["timestamp", "symbol"],
            columns="exchange",
            values="oi_growth",
            aggfunc="first",
        ).reset_index()

        exchanges = [c for c in pivot.columns if c not in ("timestamp", "symbol")]
        if len(exchanges) < 2:
            return pd.DataFrame()

        # Compute max divergence
        pivot["max_divergence"] = pivot[exchanges].max(axis=1) - pivot[exchanges].min(axis=1)
        pivot["divergence_flag"] = (
            pivot["max_divergence"] > self.params.divergence_threshold
        ).astype(int)

        return pivot

    def funding_oi_composite(
        self, oi_df: pd.DataFrame, funding_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Create composite signal combining OI concentration with funding rates.

        High funding + high OI growth = crowded trade (reduce exposure)
        High funding + low OI = sustainable opportunity (increase exposure)
        """
        oi_signals = self.compute(oi_df)
        if oi_signals.empty:
            return pd.DataFrame()

        # Merge with funding data
        merged = pd.merge(
            oi_signals[
                ["timestamp", "symbol", "exchange", "oi_zscore", "crowded_flag", "oi_percentile"]
            ],
            funding_df[["timestamp", "symbol", "exchange", "funding_rate"]],
            on=["timestamp", "symbol", "exchange"],
            how="inner",
        )

        # Composite score: high funding is good, but crowded OI is bad
        merged["funding_annualised"] = merged["funding_rate"] * 3 * 365
        merged["composite_score"] = merged["funding_annualised"] * (
            1 - 0.5 * merged["crowded_flag"]
        )

        return merged
