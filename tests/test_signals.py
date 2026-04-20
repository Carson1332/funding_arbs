"""Tests for research signal modules."""

from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd

from research.basis_momentum import BasisMomentum
from research.funding_zscore import FundingZScore, ZScoreParams
from research.term_structure import FundingTermStructure


def _make_funding_df(
    n: int = 200, rate_mean: float = 0.0001, rate_std: float = 0.00005
) -> pd.DataFrame:
    """Generate synthetic funding rate data for testing."""
    np.random.seed(42)
    timestamps = [
        datetime(2024, 1, 1, tzinfo=timezone.utc) + timedelta(hours=8 * i)
        for i in range(n)
    ]
    rates = np.random.normal(rate_mean, rate_std, n)
    # Add a spike for signal detection
    rates[150:160] = rate_mean + 3 * rate_std

    return pd.DataFrame({
        "timestamp": timestamps,
        "symbol": "BTC/USDT:USDT",
        "exchange": "binance",
        "funding_rate": rates,
    })


class TestFundingZScore:
    def test_compute_returns_dataframe(self):
        df = _make_funding_df()
        zscore = FundingZScore()
        result = zscore.compute(df)
        assert isinstance(result, pd.DataFrame)
        assert "zscore" in result.columns
        assert "signal" in result.columns

    def test_entry_signal_generated(self):
        df = _make_funding_df()
        params = ZScoreParams(lookback_periods=30, entry_threshold=1.0, min_annualised_rate=0.0)
        zscore = FundingZScore(params)
        result = zscore.compute(df)
        entries = result[result["signal"] == 1]
        assert len(entries) > 0, "Should generate at least one entry signal"

    def test_rank_opportunities(self):
        df = _make_funding_df()
        zscore = FundingZScore(ZScoreParams(min_annualised_rate=0.0))
        signals = zscore.compute(df)
        ranked = zscore.rank_opportunities(signals)
        assert isinstance(ranked, pd.DataFrame)

    def test_empty_input(self):
        empty = pd.DataFrame(columns=["timestamp", "symbol", "exchange", "funding_rate"])
        zscore = FundingZScore()
        result = zscore.compute(empty)
        assert len(result) == 0


class TestBasisMomentum:
    def test_compute(self):
        n = 100
        timestamps = [
            datetime(2024, 1, 1, tzinfo=timezone.utc) + timedelta(hours=8 * i)
            for i in range(n)
        ]
        df = pd.DataFrame({
            "timestamp": timestamps,
            "symbol": "BTC/USDT:USDT",
            "exchange": "binance",
            "basis_bps": np.random.normal(5, 2, n),
        })
        bm = BasisMomentum()
        result = bm.compute(df)
        assert "basis_momentum" in result.columns
        assert "signal" in result.columns


class TestTermStructure:
    def test_compute(self):
        df = _make_funding_df(n=200)
        ts = FundingTermStructure()
        result = ts.compute(df)
        assert "rate_short" in result.columns
        assert "rate_long" in result.columns
        assert "slope" in result.columns
        assert "regime" in result.columns

    def test_regime_detection(self):
        df = _make_funding_df(n=200)
        ts = FundingTermStructure()
        result = ts.compute(df)
        regimes = result["regime"].unique()
        assert len(regimes) > 0
