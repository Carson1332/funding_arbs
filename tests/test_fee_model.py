"""Tests for fee model."""

import pytest
from backtest.fee_model import FeeModel, FeeSchedule, EXCHANGE_FEES


class TestFeeModel:
    def test_default_binance(self):
        fm = FeeModel("binance")
        assert fm.schedule.exchange == "binance"
        assert fm.schedule.perp_taker == 0.0005

    def test_spot_trade_cost(self):
        fm = FeeModel("binance")
        cost = fm.spot_trade_cost(1.0, 50000.0)
        # Fee: 50000 * 0.001 = 50, Slippage: 50000 * 0.0001 = 5
        assert cost == pytest.approx(55.0, rel=0.01)

    def test_perp_trade_cost(self):
        fm = FeeModel("binance")
        cost = fm.perp_trade_cost(1.0, 50000.0)
        # Fee: 50000 * 0.0005 = 25, Slippage: 50000 * 0.0001 = 5
        assert cost == pytest.approx(30.0, rel=0.01)

    def test_funding_payment_short_positive(self):
        fm = FeeModel("binance")
        # Short position (-1 BTC) with positive funding rate
        payment = fm.funding_payment(-1.0, 0.0001, 50000.0)
        # -(-1 * 50000) * 0.0001 = 5.0 (short receives)
        assert payment == pytest.approx(5.0, rel=0.01)

    def test_funding_payment_short_negative(self):
        fm = FeeModel("binance")
        # Short position with negative funding rate
        payment = fm.funding_payment(-1.0, -0.0001, 50000.0)
        # -(-1 * 50000) * (-0.0001) = -5.0 (short pays)
        assert payment == pytest.approx(-5.0, rel=0.01)

    def test_entry_cost(self):
        fm = FeeModel("binance")
        cost = fm.entry_cost(1.0, 50000.0, 1.0, 50000.0)
        assert cost > 0

    def test_custom_schedule(self):
        custom = FeeSchedule(
            exchange="custom",
            spot_maker=0.0,
            spot_taker=0.0,
            perp_maker=0.0,
            perp_taker=0.0,
            slippage_bps=0.0,
        )
        fm = FeeModel(custom_schedule=custom)
        cost = fm.spot_trade_cost(1.0, 50000.0)
        assert cost == 0.0

    def test_annualised_cost_estimate(self):
        fm = FeeModel("binance")
        result = fm.annualised_cost_estimate(100_000)
        assert "net_annual_yield_pct" in result
        assert result["annual_funding_collected"] > 0

    def test_all_exchanges_defined(self):
        for exc in ["binance", "bybit", "okx"]:
            assert exc in EXCHANGE_FEES
