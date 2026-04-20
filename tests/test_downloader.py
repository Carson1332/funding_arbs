"""Tests for data downloader module."""

import pytest
import pandas as pd
from datetime import datetime, timezone

from data.downloader import FundingRateDownloader


class TestFundingRateDownloader:
    """Test suite for FundingRateDownloader."""

    def test_init_default(self):
        config = {
            "universe": ["BTC/USDT:USDT"],
            "exchanges": ["binance"],
        }
        dl = FundingRateDownloader(config)
        assert dl.pairs == ["BTC/USDT:USDT"]
        assert dl.exchange_names == ["binance"]

    def test_coinapi_symbol_conversion(self):
        result = FundingRateDownloader._to_coinapi_symbol("binance", "BTC/USDT:USDT")
        assert result == "BINANCE_PERP_BTC_USDT"

    def test_coinapi_symbol_conversion_bybit(self):
        result = FundingRateDownloader._to_coinapi_symbol("bybit", "ETH/USDT:USDT")
        assert result == "BYBIT_PERP_ETH_USDT"

    def test_load_cached_empty(self):
        """Loading from empty cache should return empty DataFrame."""
        df = FundingRateDownloader.load_cached_data(exchange="nonexistent")
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 0

    def test_cache_path_generation(self):
        from data.downloader import _cache_path
        path = _cache_path("binance", "BTC/USDT:USDT")
        assert "binance_BTC_USDT_USDT" in str(path)
        assert str(path).endswith(".parquet")
