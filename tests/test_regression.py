"""Regression tests for known bugs."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd
import pytest

from run_parameter_sweep import SweepBacktest, SweepConfig


def _make_synthetic_funding_df(n: int = 300) -> pd.DataFrame:
    """Generate synthetic funding data with enough signal for TS entry.

    Two symbols:
      - BTC: high funding (~40% ann) -> passes both strict (0.08) and moderate (0.06)
      - ETH: medium funding (~9% ann) -> passes moderate but not strict
    A positive linear trend ensures ts_slope > 0 (3d mean > 30d mean).
    """
    np.random.seed(7)
    timestamps = [
        datetime(2024, 1, 1, tzinfo=timezone.utc) + timedelta(hours=8 * i)
        for i in range(n)
    ]
    trend = np.linspace(0, 0.0001, n)
    btc_rates = np.full(n, 0.0003) + trend   # ~32-65% ann
    eth_rates = np.full(n, 0.000065) + trend  # ~7-16% ann (straddles 0.08 threshold)

    return pd.concat([
        pd.DataFrame({
            "timestamp": timestamps,
            "symbol": "BTC/USDT:USDT",
            "exchange": "binance",
            "funding_rate": btc_rates,
        }),
        pd.DataFrame({
            "timestamp": timestamps,
            "symbol": "ETH/USDT:USDT",
            "exchange": "binance",
            "funding_rate": eth_rates,
        }),
    ], ignore_index=True)


class TestCarryPlusTSParameterWiring:
    """Ensure carry_plus_ts respects config parameters (regression for hard-coded threshold)."""

    def test_strict_and_moderate_produce_different_results(self):
        """Strict (0.08) and moderate (0.06) entry thresholds must not be identical."""
        df = _make_synthetic_funding_df()

        strict_cfg = SweepConfig(
            strategy="carry_plus_ts",
            min_ann_rate_entry=0.08,
        )
        moderate_cfg = SweepConfig(
            strategy="carry_plus_ts",
            min_ann_rate_entry=0.06,
        )

        strict_bt = SweepBacktest(strict_cfg)
        strict_state = strict_bt.run(df)
        strict_trades = [t for t in strict_state.trade_log if t["action"] == "CLOSE"]

        moderate_bt = SweepBacktest(moderate_cfg)
        moderate_state = moderate_bt.run(df)
        moderate_trades = [t for t in moderate_state.trade_log if t["action"] == "CLOSE"]

        assert len(strict_trades) != len(moderate_trades) or strict_state.equity != moderate_state.equity, (
            "Strict and moderate configs produced identical results — "
            "min_ann_rate_entry is likely hard-coded and not wired through to the strategy"
        )
