"""Tests for backtest simulator."""

import pytest
import numpy as np
import pandas as pd
from datetime import datetime, timedelta, timezone

from backtest.simulator import FundingRateSimulator, SimulatorConfig
from backtest.portfolio import PortfolioState, Position


class TestPortfolioState:
    def test_initial_state(self):
        state = PortfolioState(cash=100_000)
        assert state.equity == 100_000
        assert state.position_count == 0
        assert state.can_open_position()

    def test_equity_with_positions(self):
        state = PortfolioState(cash=90_000)
        pos = Position(
            symbol="BTC/USDT:USDT",
            exchange="binance",
            entry_time=datetime.now(timezone.utc),
            spot_qty=1.0,
            spot_entry_price=50000,
            perp_qty=-1.0,
            perp_entry_price=50000,
            funding_collected=100,
            trading_costs=50,
        )
        state.positions["BTC/USDT:USDT"] = pos
        # equity = cash + net_pnl (100 - 50 + 0 unrealised = 50)
        assert state.equity == 90_050

    def test_max_positions(self):
        state = PortfolioState(max_positions=2)
        state.positions["A"] = Position("A", "binance", datetime.now(timezone.utc))
        state.positions["B"] = Position("B", "binance", datetime.now(timezone.utc))
        assert not state.can_open_position()

    def test_record_equity(self):
        state = PortfolioState(cash=100_000)
        state.record_equity(datetime.now(timezone.utc))
        assert len(state.equity_history) == 1
        assert state.equity_history[0]["equity"] == 100_000


class TestPosition:
    def test_mark_to_market(self):
        pos = Position(
            symbol="BTC/USDT:USDT",
            exchange="binance",
            entry_time=datetime.now(timezone.utc),
            spot_qty=1.0,
            spot_entry_price=50000,
            perp_qty=-1.0,
            perp_entry_price=50000,
        )
        # Price goes up: spot gains, perp loses (short)
        pnl = pos.mark_to_market(51000, 51000)
        assert pnl == 0  # delta-neutral: spot +1000, perp -1000

    def test_funding_collection(self):
        pos = Position(
            symbol="BTC/USDT:USDT",
            exchange="binance",
            entry_time=datetime.now(timezone.utc),
            funding_collected=500,
            trading_costs=100,
        )
        assert pos.net_pnl == 400


class TestSimulator:
    def _make_test_data(self, n: int = 50):
        """Create minimal test data for simulation."""
        timestamps = [
            datetime(2024, 1, 1, tzinfo=timezone.utc) + timedelta(hours=8 * i)
            for i in range(n)
        ]
        funding_df = pd.DataFrame({
            "timestamp": timestamps,
            "symbol": "BTC/USDT:USDT",
            "exchange": "binance",
            "funding_rate": np.random.uniform(0.0001, 0.0003, n),
        })
        signals_df = pd.DataFrame({
            "timestamp": timestamps,
            "symbol": "BTC/USDT:USDT",
            "exchange": "binance",
            "signal": [1] + [0] * (n - 2) + [-1],
            "zscore": [2.0] + [1.0] * (n - 2) + [-0.5],
        })
        return funding_df, signals_df

    def test_simulator_runs(self):
        config = SimulatorConfig(
            initial_capital=100_000,
            use_kalman_hedge=False,
            min_annualised_rate=0.0,
        )
        sim = FundingRateSimulator(config)
        funding_df, signals_df = self._make_test_data()
        state = sim.run(funding_df, signals_df)
        assert len(state.equity_history) > 0
        assert state.equity > 0
