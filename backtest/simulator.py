"""
Funding Rate Replay Simulator.

Replays historical 8-hour funding rate snapshots and simulates the P&L
of maintaining delta-neutral positions (long spot / short perp).

This is the core backtest engine that:
1. Iterates through 8h funding snapshots chronologically
2. Applies entry/exit signals from research modules
3. Collects funding payments on open positions
4. Adjusts hedge ratios using Kalman filter
5. Tracks all costs (fees, slippage, rebalancing)
6. Records equity curve and trade log
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional

import numpy as np
import pandas as pd

from backtest.fee_model import FeeModel
from backtest.portfolio import PortfolioState, Position
from research.kalman_hedge import DynamicHedgeRatio, KalmanHedgeParams, SimpleHedgeRatio


@dataclass
class SimulatorConfig:
    """Configuration for the backtest simulator."""

    initial_capital: float = 100_000.0
    max_positions: int = 10
    max_position_pct: float = 0.15  # max 15% of equity per position

    # Entry/exit thresholds
    entry_zscore: float = 1.5
    exit_zscore: float = 0.0
    min_annualised_rate: float = 0.05  # 5% minimum annualised funding

    # Hedge ratio
    use_kalman_hedge: bool = True
    kalman_delta: float = 1e-4
    kalman_R: float = 1.0
    rebalance_threshold: float = 0.03  # 3% drift triggers rebalance

    # Risk management
    max_drawdown_exit: float = 0.10  # exit all if drawdown > 10%
    max_single_loss: float = 0.02  # exit position if loss > 2% of equity

    # Exchange
    exchange: str = "binance"


class FundingRateSimulator:
    """
    Core backtest engine: replays funding rate history and simulates P&L.
    """

    def __init__(self, config: SimulatorConfig):
        self.config = config
        self.state = PortfolioState(
            cash=config.initial_capital,
            initial_capital=config.initial_capital,
            max_positions=config.max_positions,
            max_position_pct=config.max_position_pct,
        )
        self.fee_model = FeeModel(exchange=config.exchange)

        # Initialise hedge ratio estimator
        if config.use_kalman_hedge:
            try:
                self.hedge = DynamicHedgeRatio(
                    KalmanHedgeParams(
                        delta=config.kalman_delta,
                        R=config.kalman_R,
                        rebalance_threshold=config.rebalance_threshold,
                    )
                )
            except ImportError:
                print("filterpy not available, falling back to simple hedge ratio")
                self.hedge = SimpleHedgeRatio()
        else:
            self.hedge = SimpleHedgeRatio()

    def run(
        self,
        funding_df: pd.DataFrame,
        signals_df: pd.DataFrame,
        prices_df: Optional[pd.DataFrame] = None,
    ) -> PortfolioState:
        """
        Run the full backtest simulation.

        Parameters
        ----------
        funding_df : pd.DataFrame
            Funding rate data with columns: timestamp, symbol, exchange, funding_rate
        signals_df : pd.DataFrame
            Signal data with columns: timestamp, symbol, exchange, signal, zscore
        prices_df : pd.DataFrame, optional
            Price data with columns: timestamp, symbol, spot_close, perp_close
            If None, uses funding_df timestamps with synthetic prices.

        Returns
        -------
        PortfolioState : final portfolio state with equity history
        """
        # Sort by timestamp
        funding_df = funding_df.sort_values("timestamp").reset_index(drop=True)
        signals_df = signals_df.sort_values("timestamp").reset_index(drop=True)

        # Get unique timestamps (8h intervals)
        timestamps = sorted(funding_df["timestamp"].unique())

        print(f"Running simulation: {len(timestamps)} periods, "
              f"{funding_df['symbol'].nunique()} symbols")

        for i, ts in enumerate(timestamps):
            # Get data for this timestamp
            funding_snapshot = funding_df[funding_df["timestamp"] == ts]
            signal_snapshot = signals_df[signals_df["timestamp"] == ts]

            # Get prices if available
            price_snapshot = None
            if prices_df is not None:
                price_snapshot = prices_df[prices_df["timestamp"] == ts]

            # Step 1: Collect funding on existing positions
            self._collect_funding(funding_snapshot)

            # Step 2: Adjust hedge ratios if we have price data
            if price_snapshot is not None and not price_snapshot.empty:
                self._adjust_hedges(price_snapshot)

            # Step 3: Check risk limits
            self._check_risk_limits(ts, price_snapshot)

            # Step 4: Process signals (enter/exit)
            self._process_signals(ts, signal_snapshot, funding_snapshot, price_snapshot)

            # Step 5: Mark to market
            if price_snapshot is not None and not price_snapshot.empty:
                self._mark_to_market(price_snapshot)

            # Step 6: Record equity
            self.state.record_equity(ts)

            # Progress
            if (i + 1) % 100 == 0:
                print(
                    f"  Period {i+1}/{len(timestamps)}: "
                    f"equity=${self.state.equity:,.0f}, "
                    f"positions={self.state.position_count}, "
                    f"dd={self.state.drawdown:.2%}"
                )

        print(f"\nSimulation complete. Final equity: ${self.state.equity:,.0f}")
        return self.state

    def _collect_funding(self, snapshot: pd.DataFrame) -> None:
        """Collect funding payments on all open positions."""
        for symbol, pos in self.state.positions.items():
            row = snapshot[
                (snapshot["symbol"] == symbol) & (snapshot["exchange"] == pos.exchange)
            ]
            if row.empty:
                continue

            rate = row.iloc[0]["funding_rate"]
            # Short perp collects positive funding, pays negative
            payment = self.fee_model.funding_payment(
                pos.perp_qty, rate, pos.entry_basis
            )
            pos.funding_collected += payment

    def _adjust_hedges(self, prices: pd.DataFrame) -> None:
        """Re-estimate hedge ratio and rebalance if drift exceeds threshold."""
        for symbol, pos in list(self.state.positions.items()):
            price_row = prices[prices["symbol"] == symbol]
            if price_row.empty:
                continue

            spot_price = price_row.iloc[0].get("spot_close", pos.spot_entry_price)
            perp_price = price_row.iloc[0].get("perp_close", pos.perp_entry_price)

            # Update hedge ratio
            if hasattr(self.hedge, "update"):
                new_ratio = self.hedge.update(spot_price, perp_price)
            else:
                new_ratio = pos.hedge_ratio  # keep existing

            target_perp = -pos.spot_qty * new_ratio
            current_perp = pos.perp_qty

            if abs(current_perp) > 0:
                drift = abs(target_perp - current_perp) / abs(current_perp)
            else:
                drift = 0

            if drift > self.config.rebalance_threshold:
                adjustment = target_perp - current_perp
                cost = self.fee_model.rebalance_cost(abs(adjustment), perp_price)
                pos.perp_qty = target_perp
                pos.hedge_ratio = new_ratio
                pos.trading_costs += cost

    def _check_risk_limits(self, ts: datetime, prices: Optional[pd.DataFrame]) -> None:
        """Exit positions that breach risk limits."""
        # Portfolio-level drawdown check
        if (self.state.drawdown > self.config.max_drawdown_exit and
                self.state.positions):
            print(f"  [RISK] Max drawdown breached at {ts}, closing all positions")
            for symbol in list(self.state.positions.keys()):
                self._close_position(ts, symbol, "max_drawdown")
            return

        # Per-position loss check
        for symbol, pos in list(self.state.positions.items()):
            if pos.net_pnl < -self.state.equity * self.config.max_single_loss:
                self._close_position(ts, symbol, "max_single_loss")

    def _process_signals(
        self,
        ts: datetime,
        signals: pd.DataFrame,
        funding: pd.DataFrame,
        prices: Optional[pd.DataFrame],
    ) -> None:
        """Process entry/exit signals."""
        if signals.empty:
            return

        for _, row in signals.iterrows():
            symbol = row["symbol"]
            exchange = row.get("exchange", self.config.exchange)
            signal = row.get("signal", 0)
            zscore = row.get("zscore", 0)

            if signal == 1 and symbol not in self.state.positions:
                # Entry signal
                if self.state.drawdown >= self.config.max_drawdown_exit:
                    continue  # suppress entries during drawdown recovery
                if not self.state.can_open_position():
                    continue

                # Get prices
                funding_rate = 0.0
                fr = funding[(funding["symbol"] == symbol) & (funding["exchange"] == exchange)]
                if not fr.empty:
                    funding_rate = fr.iloc[0]["funding_rate"]

                annualised = funding_rate * 3 * 365
                if annualised < self.config.min_annualised_rate:
                    continue

                self._open_position(ts, symbol, exchange, funding_rate, prices)

            elif signal == -1 and symbol in self.state.positions:
                # Exit signal
                self._close_position(ts, symbol, "signal_exit")

    def _open_position(
        self,
        ts: datetime,
        symbol: str,
        exchange: str,
        funding_rate: float,
        prices: Optional[pd.DataFrame],
    ) -> None:
        """Open a new delta-neutral position."""
        size = self.state.position_size()
        if size <= 0:
            return

        # Get prices (use synthetic if not available)
        spot_price = 50000.0  # default
        perp_price = 50000.0
        if prices is not None and not prices.empty:
            pr = prices[prices["symbol"] == symbol]
            if not pr.empty:
                spot_price = pr.iloc[0].get("spot_close", 50000.0)
                perp_price = pr.iloc[0].get("perp_close", spot_price)

        spot_qty = size / spot_price
        perp_qty = -spot_qty  # short perp

        # Calculate entry costs
        cost = self.fee_model.entry_cost(spot_qty, spot_price, abs(perp_qty), perp_price)

        pos = Position(
            symbol=symbol,
            exchange=exchange,
            entry_time=ts,
            spot_qty=spot_qty,
            spot_entry_price=spot_price,
            perp_qty=perp_qty,
            perp_entry_price=perp_price,
            hedge_ratio=1.0,
            entry_basis=perp_price,
            trading_costs=cost,
        )

        self.state.positions[symbol] = pos

        self.state.log_trade(
            ts, symbol, "OPEN",
            {
                "spot_qty": spot_qty,
                "perp_qty": perp_qty,
                "spot_price": spot_price,
                "perp_price": perp_price,
                "cost": cost,
                "funding_rate": funding_rate,
            },
        )

    def _close_position(self, ts: datetime, symbol: str, reason: str) -> None:
        """Close an existing position."""
        if symbol not in self.state.positions:
            return

        pos = self.state.positions[symbol]

        # Exit costs (use entry prices as approximation if no current prices)
        cost = self.fee_model.exit_cost(
            pos.spot_qty, pos.spot_entry_price,
            abs(pos.perp_qty), pos.perp_entry_price,
        )

        # Realise P&L: net_pnl already tracks funding - costs + unrealised.
        # We only need to add it to cash (minus exit cost) since cash wasn't
        # adjusted during open/hold/rebalance.
        realised_pnl = pos.net_pnl - cost
        self.state.cash += realised_pnl

        self.state.log_trade(
            ts, symbol, "CLOSE",
            {
                "reason": reason,
                "funding_collected": pos.funding_collected,
                "trading_costs": pos.trading_costs + cost,
                "net_pnl": realised_pnl,
                "hold_periods": None,  # could compute from entry_time
            },
        )

        del self.state.positions[symbol]

    def _mark_to_market(self, prices: pd.DataFrame) -> None:
        """Update unrealised P&L for all positions."""
        for symbol, pos in self.state.positions.items():
            pr = prices[prices["symbol"] == symbol]
            if pr.empty:
                continue
            spot_price = pr.iloc[0].get("spot_close", pos.spot_entry_price)
            perp_price = pr.iloc[0].get("perp_close", pos.perp_entry_price)
            pos.mark_to_market(spot_price, perp_price)
