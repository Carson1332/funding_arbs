"""
Funding Rate Z-Score Signal.

Computes a z-score on rolling funding rates to identify when funding is
abnormally high (opportunity to short perp and collect funding) or
abnormally low (exit signal).

Signal logic:
    z = (current_rate - rolling_mean) / rolling_std

    z > entry_threshold  →  ENTER long-spot / short-perp
    z < exit_threshold   →  EXIT position
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import numpy as np
import pandas as pd


@dataclass
class ZScoreParams:
    """Parameters for the funding rate z-score signal."""

    lookback_periods: int = 90  # number of 8h periods (~30 days)
    entry_threshold: float = 1.5  # z-score to enter
    exit_threshold: float = 0.0  # z-score to exit
    min_annualised_rate: float = 0.05  # minimum 5% annualised to consider


class FundingZScore:
    """
    Compute z-score signals on funding rate time series.

    Designed to work on a per-symbol, per-exchange basis or cross-exchange.
    """

    def __init__(self, params: Optional[ZScoreParams] = None):
        self.params = params or ZScoreParams()

    def compute(self, funding_df: pd.DataFrame) -> pd.DataFrame:
        """
        Compute z-score for each symbol/exchange combination.

        Parameters
        ----------
        funding_df : pd.DataFrame
            Must have columns: timestamp, symbol, exchange, funding_rate

        Returns
        -------
        pd.DataFrame with additional columns:
            rolling_mean, rolling_std, zscore, signal
        """
        df = funding_df.copy()
        df = df.sort_values(["symbol", "exchange", "timestamp"])

        results = []
        for (symbol, exchange), group in df.groupby(["symbol", "exchange"]):
            g = group.copy()
            g["rolling_mean"] = g["funding_rate"].rolling(
                self.params.lookback_periods, min_periods=10
            ).mean()
            g["rolling_std"] = g["funding_rate"].rolling(
                self.params.lookback_periods, min_periods=10
            ).std()
            g["zscore"] = (
                (g["funding_rate"] - g["rolling_mean"])
                / g["rolling_std"].replace(0, np.nan)
            )

            # Annualised rate filter
            g["annualised_rate"] = g["funding_rate"] * 3 * 365

            # Generate signal
            g["signal"] = 0  # 0 = no position
            g.loc[
                (g["zscore"] > self.params.entry_threshold)
                & (g["annualised_rate"] > self.params.min_annualised_rate),
                "signal",
            ] = 1  # 1 = enter long-spot/short-perp
            g.loc[g["zscore"] < self.params.exit_threshold, "signal"] = -1  # -1 = exit

            results.append(g)

        if not results:
            return pd.DataFrame()
        return pd.concat(results, ignore_index=True)

    def rank_opportunities(self, signals_df: pd.DataFrame) -> pd.DataFrame:
        """
        Rank current opportunities by z-score across all symbols.

        Returns the latest signal for each symbol, sorted by z-score descending.
        """
        if signals_df.empty:
            return pd.DataFrame()

        latest = (
            signals_df.sort_values("timestamp")
            .groupby(["symbol", "exchange"])
            .last()
            .reset_index()
        )
        latest = latest[latest["signal"] == 1]
        return latest.sort_values("zscore", ascending=False).reset_index(drop=True)

    def cross_exchange_spread(self, funding_df: pd.DataFrame) -> pd.DataFrame:
        """
        Compute cross-exchange funding rate spread for the same symbol.

        Identifies pairs where funding diverges between exchanges —
        opportunity for cross-exchange arb.
        """
        df = funding_df.copy()
        df = df.sort_values("timestamp")

        # Pivot to get funding rates per exchange
        pivot = df.pivot_table(
            index=["timestamp", "symbol"],
            columns="exchange",
            values="funding_rate",
            aggfunc="first",
        ).reset_index()

        exchanges = [c for c in pivot.columns if c not in ("timestamp", "symbol")]
        if len(exchanges) < 2:
            return pd.DataFrame()

        # Compute pairwise spreads
        spreads = []
        for i, exc_a in enumerate(exchanges):
            for exc_b in exchanges[i + 1 :]:
                temp = pivot[["timestamp", "symbol"]].copy()
                temp["exchange_a"] = exc_a
                temp["exchange_b"] = exc_b
                temp["rate_a"] = pivot[exc_a]
                temp["rate_b"] = pivot[exc_b]
                temp["spread"] = pivot[exc_a] - pivot[exc_b]
                temp["spread_annualised"] = temp["spread"] * 3 * 365
                spreads.append(temp.dropna())

        if not spreads:
            return pd.DataFrame()
        return pd.concat(spreads, ignore_index=True)
