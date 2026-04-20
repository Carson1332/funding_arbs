"""
Fee Model for Backtest Simulation.

Models all trading costs including:
- Spot maker/taker fees
- Perpetual maker/taker fees
- Funding rate payments
- Slippage (market impact)
- Withdrawal/transfer fees

Default fee schedules are based on typical VIP-0 tier rates.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass
class FeeSchedule:
    """Fee schedule for a single exchange."""

    exchange: str = "binance"

    # Spot fees (as decimal, e.g., 0.001 = 0.1%)
    spot_maker: float = 0.0010
    spot_taker: float = 0.0010

    # Perpetual fees
    perp_maker: float = 0.0002
    perp_taker: float = 0.0005

    # Slippage model
    slippage_bps: float = 1.0  # basis points of slippage per trade

    # Transfer fees (for cross-exchange arb)
    transfer_fee_usd: float = 1.0  # flat fee per transfer

    # Margin/borrowing cost (annualised)
    margin_rate_annual: float = 0.0  # 0% if using own capital


# Pre-configured fee schedules for major exchanges
EXCHANGE_FEES = {
    "binance": FeeSchedule(
        exchange="binance",
        spot_maker=0.0010,
        spot_taker=0.0010,
        perp_maker=0.0002,
        perp_taker=0.0005,
        slippage_bps=1.0,
    ),
    "bybit": FeeSchedule(
        exchange="bybit",
        spot_maker=0.0010,
        spot_taker=0.0010,
        perp_maker=0.0002,
        perp_taker=0.0006,
        slippage_bps=1.5,
    ),
    "okx": FeeSchedule(
        exchange="okx",
        spot_maker=0.0008,
        spot_taker=0.0010,
        perp_maker=0.0002,
        perp_taker=0.0005,
        slippage_bps=1.0,
    ),
}


class FeeModel:
    """
    Compute trading costs for backtest simulation.

    Supports both single-exchange and cross-exchange fee calculations.
    """

    def __init__(self, exchange: str = "binance", custom_schedule: Optional[FeeSchedule] = None):
        if custom_schedule:
            self.schedule = custom_schedule
        else:
            self.schedule = EXCHANGE_FEES.get(exchange, FeeSchedule(exchange=exchange))

    def spot_trade_cost(self, quantity: float, price: float, is_maker: bool = False) -> float:
        """
        Compute cost of a spot trade.

        Parameters
        ----------
        quantity : float - amount of base currency
        price : float - price per unit
        is_maker : bool - True if maker order

        Returns
        -------
        float : total cost in quote currency (fee + slippage)
        """
        notional = quantity * price
        fee_rate = self.schedule.spot_maker if is_maker else self.schedule.spot_taker
        fee = notional * fee_rate
        slippage = notional * (self.schedule.slippage_bps / 10000)
        return fee + slippage

    def perp_trade_cost(self, quantity: float, price: float, is_maker: bool = False) -> float:
        """
        Compute cost of a perpetual swap trade.

        Parameters
        ----------
        quantity : float - amount of base currency
        price : float - price per unit
        is_maker : bool - True if maker order

        Returns
        -------
        float : total cost in quote currency (fee + slippage)
        """
        notional = quantity * price
        fee_rate = self.schedule.perp_maker if is_maker else self.schedule.perp_taker
        fee = notional * fee_rate
        slippage = notional * (self.schedule.slippage_bps / 10000)
        return fee + slippage

    def entry_cost(
        self, spot_qty: float, spot_price: float, perp_qty: float, perp_price: float
    ) -> float:
        """
        Total cost to enter a delta-neutral position (buy spot + short perp).
        """
        return (
            self.spot_trade_cost(spot_qty, spot_price, is_maker=False)
            + self.perp_trade_cost(perp_qty, perp_price, is_maker=False)
        )

    def exit_cost(
        self, spot_qty: float, spot_price: float, perp_qty: float, perp_price: float
    ) -> float:
        """
        Total cost to exit a delta-neutral position (sell spot + close perp short).
        """
        return (
            self.spot_trade_cost(spot_qty, spot_price, is_maker=False)
            + self.perp_trade_cost(perp_qty, perp_price, is_maker=False)
        )

    def funding_payment(self, perp_qty: float, funding_rate: float, mark_price: float) -> float:
        """
        Compute funding payment for a perpetual position.

        For a SHORT position with positive funding rate, the short RECEIVES funding.
        For a SHORT position with negative funding rate, the short PAYS funding.

        Parameters
        ----------
        perp_qty : float - signed quantity (negative = short)
        funding_rate : float - funding rate as decimal
        mark_price : float - mark/index price

        Returns
        -------
        float : funding payment (positive = received, negative = paid)
        """
        # Funding payment = -position_value * funding_rate
        # Short position (negative qty) with positive rate → receives payment
        position_value = perp_qty * mark_price
        return -position_value * funding_rate

    def rebalance_cost(self, adjustment_qty: float, price: float) -> float:
        """Cost of rebalancing the perp side of the hedge."""
        return self.perp_trade_cost(abs(adjustment_qty), price, is_maker=False)

    def annualised_cost_estimate(
        self,
        notional: float,
        trades_per_year: int = 4,
        avg_funding_rate: float = 0.0001,
    ) -> dict:
        """
        Estimate annualised costs for a given notional position.

        Returns breakdown of costs as a dictionary.
        """
        # Entry + exit costs (assume 1 round trip per position)
        entry_exit = self.entry_cost(
            notional / 50000, 50000, notional / 50000, 50000
        ) * trades_per_year * 2

        # Funding collected (assuming short perp with positive rate)
        funding_collected = notional * avg_funding_rate * 3 * 365

        return {
            "notional_usd": notional,
            "annual_trading_costs": round(entry_exit, 2),
            "annual_funding_collected": round(funding_collected, 2),
            "net_annual_pnl": round(funding_collected - entry_exit, 2),
            "net_annual_yield_pct": round(
                (funding_collected - entry_exit) / notional * 100, 2
            ),
        }
