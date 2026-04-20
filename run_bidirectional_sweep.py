"""
Bidirectional funding rate arbitrage backtest.

Harvests yield from both positive and negative funding regimes:
- Positive funding (> +8% ann): long spot + short perp
- Negative funding (< -8% ann): short spot + long perp

Borrow costs for short spot are modelled explicitly.
"""
from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

from backtest.enhanced_engine import (
    compute_funding_features,
    load_all_funding_data,
)


@dataclass
class BidirectionalConfig:
    initial_capital: float = 100_000.0
    max_positions: int = 8
    max_position_pct: float = 0.20
    max_drawdown_exit: float = 0.08
    max_single_loss_pct: float = 0.015

    # Fee model
    spot_taker_fee: float = 0.0010
    perp_taker_fee: float = 0.0005
    slippage_bps: float = 1.0

    # Borrow cost for shorting spot (annualised)
    borrow_cost_annual: float = 0.08  # 8% per year to borrow spot

    # Strategy-specific
    strategy: str = "bidirectional_carry"

    # Positive carry params
    pos_min_ann_rate: float = 0.08
    pos_min_positive_streak: int = 6
    pos_exit_ann_rate: float = 0.02
    pos_exit_momentum_threshold: float = -0.00008
    pos_max_hold_periods: int = 180

    # Negative carry params (symmetric)
    neg_min_ann_rate: float = -0.08
    neg_min_negative_streak: int = 6
    neg_exit_ann_rate: float = -0.02
    neg_exit_momentum_threshold: float = 0.00008
    neg_max_hold_periods: int = 180
    neg_min_hold_periods: int = 3  # minimum periods before signal exit


@dataclass
class CarryPosition:
    symbol: str
    exchange: str
    entry_time: datetime
    collateral: float
    entry_cost: float
    funding_collected: float = 0.0
    borrow_paid: float = 0.0
    periods_held: int = 0
    peak_pnl: float = 0.0
    signal_strength: float = 1.0
    direction: str = "long"  # "long" = long spot / short perp; "short" = short spot / long perp

    @property
    def net_pnl(self) -> float:
        return self.funding_collected - self.borrow_paid - self.entry_cost

    @property
    def return_on_collateral(self) -> float:
        return self.net_pnl / self.collateral if self.collateral > 0 else 0


@dataclass
class PortfolioState:
    initial_capital: float = 100_000.0
    positions: dict = field(default_factory=dict)
    equity_history: list = field(default_factory=list)
    trade_log: list = field(default_factory=list)
    _cumulative_realised_pnl: float = 0.0

    @property
    def locked_collateral(self) -> float:
        return sum(p.collateral for p in self.positions.values())

    @property
    def unrealised_pnl(self) -> float:
        return sum(p.net_pnl for p in self.positions.values())

    @property
    def equity(self) -> float:
        return self.initial_capital + self._cumulative_realised_pnl + self.unrealised_pnl

    @property
    def available_capital(self) -> float:
        return self.equity - self.locked_collateral

    @property
    def drawdown(self) -> float:
        if not self.equity_history:
            return 0.0
        peak = max(e["equity"] for e in self.equity_history)
        return max(0, (peak - self.equity) / peak) if peak > 0 else 0.0


class BidirectionalBacktest:
    def __init__(self, config: BidirectionalConfig):
        self.config = config
        self.state = PortfolioState(initial_capital=config.initial_capital)

    def _entry_cost(self, collateral: float) -> float:
        spot = collateral * self.config.spot_taker_fee
        perp = collateral * self.config.perp_taker_fee
        slip = collateral * (self.config.slippage_bps / 10000) * 2
        return spot + perp + slip

    def _exit_cost(self, collateral: float) -> float:
        return self._entry_cost(collateral)

    def _position_size(self, signal_strength: float = 1.0) -> float:
        max_by_equity = self.state.equity * self.config.max_position_pct
        available = self.state.available_capital * 0.9
        base_size = max(0, min(max_by_equity, available))
        scale = 0.5 + 0.5 * min(signal_strength, 1.0)
        return base_size * scale

    def _can_open(self) -> bool:
        return (
            len(self.state.positions) < self.config.max_positions
            and self.state.available_capital > self.state.equity * 0.05
        )

    def _open(self, ts, symbol, exchange, rate, signal_strength, direction):
        collateral = self._position_size(signal_strength)
        if collateral < 100:
            return
        cost = self._entry_cost(collateral)
        key = f"{symbol}_{exchange}_{direction}"
        self.state.positions[key] = CarryPosition(
            symbol=symbol,
            exchange=exchange,
            entry_time=ts,
            collateral=collateral,
            entry_cost=cost,
            signal_strength=signal_strength,
            direction=direction,
        )
        self.state.trade_log.append({
            "timestamp": ts,
            "symbol": symbol,
            "action": "OPEN",
            "direction": direction,
            "collateral": collateral,
            "entry_cost": cost,
            "funding_rate": rate,
            "signal_strength": signal_strength,
            "equity": self.state.equity,
        })

    def _close(self, ts, key, reason="signal"):
        if key not in self.state.positions:
            return
        pos = self.state.positions[key]
        exit_cost = self._exit_cost(pos.collateral)
        realised = pos.net_pnl - exit_cost
        self.state._cumulative_realised_pnl += realised
        self.state.trade_log.append({
            "timestamp": ts,
            "symbol": pos.symbol,
            "action": "CLOSE",
            "direction": pos.direction,
            "reason": reason,
            "collateral": pos.collateral,
            "funding_collected": pos.funding_collected,
            "borrow_paid": pos.borrow_paid,
            "total_costs": pos.entry_cost + exit_cost,
            "net_pnl": realised,
            "periods_held": pos.periods_held,
            "return_pct": realised / pos.collateral * 100,
            "equity": self.state.equity,
        })
        del self.state.positions[key]

    def _collect_funding_and_borrow(self, snap):
        """Collect funding payments and pay borrow costs for all positions."""
        for key, pos in list(self.state.positions.items()):
            row = snap[snap["symbol"] == pos.symbol]
            if row.empty:
                continue
            rate = row.iloc[0]["funding_rate"]

            if pos.direction == "long":
                # Long spot / short perp: receive funding when rate > 0, pay when < 0
                pos.funding_collected += pos.collateral * rate
            else:
                # Short spot / long perp: receive funding when rate < 0, pay when > 0
                pos.funding_collected += pos.collateral * (-rate)

            # Borrow cost for short spot (only for short direction)
            if pos.direction == "short":
                borrow_per_period = self.config.borrow_cost_annual / 1095
                pos.borrow_paid += pos.collateral * borrow_per_period

            pos.periods_held += 1
            pos.peak_pnl = max(pos.peak_pnl, pos.net_pnl)

    def _check_risk_and_exit(self, ts, snap):
        """Portfolio-level drawdown check."""
        if self.state.drawdown > self.config.max_drawdown_exit and self.state.positions:
            for key in list(self.state.positions.keys()):
                self._close(ts, key, "max_drawdown")
            return True
        return False

    def _process_exits(self, ts, snap):
        """Process per-position exit signals."""
        for key, pos in list(self.state.positions.items()):
            row = snap[snap["symbol"] == pos.symbol]
            if row.empty:
                continue
            r = row.iloc[0]

            # Loss limit
            if pos.net_pnl < -self.state.equity * self.config.max_single_loss_pct:
                self._close(ts, key, "max_loss")
                continue

            if pos.direction == "long":
                # Long spot / short perp exits
                ann_7d = r["fr_mean_7d"] * 3 * 365 if pd.notna(r.get("fr_mean_7d")) else None
                mom_7d = r["fr_momentum_7d"] if pd.notna(r.get("fr_momentum_7d")) else None

                if ann_7d is not None and ann_7d < self.config.pos_exit_ann_rate:
                    self._close(ts, key, "low_rate")
                    continue

                if mom_7d is not None and mom_7d < self.config.pos_exit_momentum_threshold:
                    self._close(ts, key, "neg_momentum")
                    continue

                if pos.periods_held > self.config.pos_max_hold_periods:
                    self._close(ts, key, "max_hold")
                    continue

            else:
                # Short spot / long perp exits (symmetric logic)
                ann_7d = r["fr_mean_7d"] * 3 * 365 if pd.notna(r.get("fr_mean_7d")) else None
                mom_7d = r["fr_momentum_7d"] if pd.notna(r.get("fr_momentum_7d")) else None

                # Minimum hold period to avoid round-trip fee bleed on noise
                if pos.periods_held < self.config.neg_min_hold_periods:
                    pass  # skip signal exits, but max_hold still checked below
                else:
                    if ann_7d is not None and ann_7d > self.config.neg_exit_ann_rate:
                        self._close(ts, key, "low_rate")
                        continue

                    if mom_7d is not None and mom_7d > self.config.neg_exit_momentum_threshold:
                        self._close(ts, key, "pos_momentum")
                        continue

                if pos.periods_held > self.config.neg_max_hold_periods:
                    self._close(ts, key, "max_hold")
                    continue

    def _process_entries(self, ts, snap):
        """Process entry signals for both directions."""
        if not self._can_open():
            return

        candidates_long = []
        candidates_short = []

        for _, r in snap.iterrows():
            sym = r["symbol"]

            # Long spot / short perp candidates (positive funding)
            key_long = f"{sym}_binance_long"
            if key_long not in self.state.positions:
                if self._qualifies_long_entry(r):
                    score, strength = self._score_long_entry(r)
                    if score > 0:
                        candidates_long.append((sym, r, score, strength))

            # Short spot / long perp candidates (negative funding)
            key_short = f"{sym}_binance_short"
            if key_short not in self.state.positions:
                if self._qualifies_short_entry(r):
                    score, strength = self._score_short_entry(r)
                    if score > 0:
                        candidates_short.append((sym, r, score, strength))

        # Open long positions
        candidates_long.sort(key=lambda x: x[2], reverse=True)
        for sym, r, score, strength in candidates_long:
            if not self._can_open():
                break
            self._open(ts, sym, "binance", r["funding_rate"], strength, "long")

        # Open short positions
        candidates_short.sort(key=lambda x: x[2], reverse=True)
        for sym, r, score, strength in candidates_short:
            if not self._can_open():
                break
            self._open(ts, sym, "binance", r["funding_rate"], strength, "short")

    def _qualifies_long_entry(self, r) -> bool:
        if pd.isna(r.get("fr_mean_7d")) or pd.isna(r.get("fr_momentum_3d")):
            return False
        ann_7d = r["fr_mean_7d"] * 3 * 365
        if ann_7d < self.config.pos_min_ann_rate:
            return False
        if r["fr_momentum_3d"] < 0:
            return False
        streak = r.get("positive_streak")
        if pd.notna(streak) and streak < self.config.pos_min_positive_streak:
            return False
        if pd.notna(r.get("ema_crossover")) and r["ema_crossover"] < 0:
            return False
        return True

    def _score_long_entry(self, r):
        ann_7d = r["fr_mean_7d"] * 3 * 365
        strength = min(ann_7d / 0.20, 1.0)
        if r["fr_momentum_3d"] > 0.00005:
            strength *= 1.2
        score = ann_7d + r["fr_momentum_3d"] * 3000
        return score, strength

    def _qualifies_short_entry(self, r) -> bool:
        if self.config.neg_min_ann_rate > 0:
            return False  # short entries disabled
        if (
            pd.isna(r.get("fr_mean_7d"))
            or pd.isna(r.get("fr_momentum_3d"))
            or pd.isna(r.get("fr_momentum_7d"))
        ):
            return False
        ann_7d = r["fr_mean_7d"] * 3 * 365
        if ann_7d > self.config.neg_min_ann_rate:
            return False
        # Both short-term and medium-term momentum must be negative
        if r["fr_momentum_3d"] > 0:
            return False
        if r["fr_momentum_7d"] > 0:
            return False
        # Require sustained negative funding (same discipline as longs)
        streak = r.get("negative_streak")
        if pd.notna(streak) and streak < self.config.neg_min_negative_streak:
            return False
        return True

    def _score_short_entry(self, r):
        ann_7d = r["fr_mean_7d"] * 3 * 365
        strength = min(abs(ann_7d) / 0.20, 1.0)
        # Reward stronger negative momentum on both timeframes
        if r["fr_momentum_3d"] < -0.00005 and r["fr_momentum_7d"] < -0.00005:
            strength *= 1.2
        score = abs(ann_7d) + abs(r["fr_momentum_7d"]) * 3000
        return score, strength

    def run_bidirectional_carry(self, df: pd.DataFrame) -> PortfolioState:
        """Run bidirectional adaptive carry strategy."""
        features = compute_funding_features(df)
        binance = features[features["exchange"] == "binance"].copy()
        timestamps = sorted(binance["timestamp"].unique())

        print(
            f"Running Bidirectional Carry: {len(timestamps)} periods, "
            f"{binance['symbol'].nunique()} symbols"
        )

        for i, ts in enumerate(timestamps):
            snap = binance[binance["timestamp"] == ts]

            # 1. Collect funding and borrow costs
            self._collect_funding_and_borrow(snap)

            # 2. Portfolio risk
            if self._check_risk_and_exit(ts, snap):
                pass  # All positions closed

            # 3. Exit signals
            self._process_exits(ts, snap)

            # 4. Entry signals
            self._process_entries(ts, snap)

            # 5. Record
            self.state.equity_history.append({
                "timestamp": ts,
                "equity": self.state.equity,
                "positions": len(self.state.positions),
                "drawdown": self.state.drawdown,
                "locked": self.state.locked_collateral,
            })

            if (i + 1) % 500 == 0:
                print(
                    f"  Period {i+1}/{len(timestamps)}: equity=${self.state.equity:,.0f}, "
                    f"pos={len(self.state.positions)}, dd={self.state.drawdown:.2%}"
                )

        return self.state

    def run(self, df: pd.DataFrame) -> PortfolioState:
        strategies = {
            "bidirectional_carry": self.run_bidirectional_carry,
        }
        runner = strategies.get(self.config.strategy)
        if not runner:
            raise ValueError(f"Unknown strategy: {self.config.strategy}")
        return runner(df)


def compute_metrics(state: PortfolioState) -> dict:
    if not state.equity_history:
        return {"error": "No equity history"}

    eq_df = pd.DataFrame(state.equity_history)
    eq_df["timestamp"] = pd.to_datetime(eq_df["timestamp"])
    eq_df = eq_df.set_index("timestamp").sort_index()

    equity = eq_df["equity"]
    returns = equity.pct_change().dropna()

    if len(returns) < 2:
        return {"error": "Insufficient data"}

    periods_per_year = 1095
    total_return = (equity.iloc[-1] / equity.iloc[0]) - 1
    n_years = len(returns) / periods_per_year
    cagr = (1 + total_return) ** (1 / n_years) - 1 if n_years > 0 and total_return > -1 else -1

    excess_mean = returns.mean()
    excess_std = returns.std()
    sharpe = np.sqrt(periods_per_year) * excess_mean / excess_std if excess_std > 0 else 0

    downside = returns[returns < 0]
    downside_std = downside.std() if len(downside) > 0 else 0
    sortino = np.sqrt(periods_per_year) * excess_mean / downside_std if downside_std > 0 else 0

    cumulative = (1 + returns).cumprod()
    peak = cumulative.cummax()
    dd = (cumulative - peak) / peak
    max_dd = dd.min()

    calmar = cagr / abs(max_dd) if max_dd != 0 else 0

    trade_df = pd.DataFrame(state.trade_log) if state.trade_log else pd.DataFrame()
    total_trades = len(trade_df)
    win_rate = avg_win = avg_loss = profit_factor = 0

    if not trade_df.empty and "net_pnl" in trade_df.columns:
        closes = trade_df[trade_df["action"] == "CLOSE"]
        if len(closes) > 0:
            winners = closes[closes["net_pnl"] > 0]
            losers = closes[closes["net_pnl"] <= 0]
            win_rate = len(winners) / len(closes)
            avg_win = winners["net_pnl"].mean() if len(winners) > 0 else 0
            avg_loss = losers["net_pnl"].mean() if len(losers) > 0 else 0
            tw = winners["net_pnl"].sum() if len(winners) > 0 else 0
            tl = abs(losers["net_pnl"].sum()) if len(losers) > 0 else 0
            profit_factor = tw / tl if tl > 0 else float("inf")

    avg_periods_held = 0
    if not trade_df.empty and "periods_held" in trade_df.columns:
        closes = trade_df[trade_df["action"] == "CLOSE"]
        if len(closes) > 0:
            avg_periods_held = closes["periods_held"].mean()

    # Direction breakdown
    long_trades = 0
    short_trades = 0
    long_win_rate = 0
    short_win_rate = 0
    if not trade_df.empty and "direction" in trade_df.columns:
        closes = trade_df[trade_df["action"] == "CLOSE"]
        long_closes = closes[closes["direction"] == "long"]
        short_closes = closes[closes["direction"] == "short"]
        long_trades = len(long_closes)
        short_trades = len(short_closes)
        if len(long_closes) > 0:
            long_win_rate = len(long_closes[long_closes["net_pnl"] > 0]) / len(long_closes)
        if len(short_closes) > 0:
            short_win_rate = len(short_closes[short_closes["net_pnl"] > 0]) / len(short_closes)

    return {
        "sharpe": round(float(sharpe), 2),
        "annual_return_pct": round(float(cagr) * 100, 2),
        "max_drawdown_pct": round(float(max_dd) * 100, 2),
        "sortino": round(float(sortino), 2),
        "calmar": round(float(calmar), 2),
        "total_return_pct": round(float(total_return) * 100, 2),
        "total_trades": total_trades,
        "win_rate_pct": round(float(win_rate) * 100, 1),
        "avg_win": round(float(avg_win), 2),
        "avg_loss": round(float(avg_loss), 2),
        "profit_factor": round(float(profit_factor), 2) if profit_factor != float("inf") else "inf",
        "avg_hold_periods": round(float(avg_periods_held), 1),
        "avg_hold_days": round(float(avg_periods_held) / 3, 1),
        "n_years": round(n_years, 2),
        "final_equity": round(float(equity.iloc[-1]), 2),
        "initial_capital": round(float(state.initial_capital), 2),
        "long_trades": long_trades,
        "short_trades": short_trades,
        "long_win_rate_pct": round(float(long_win_rate) * 100, 1),
        "short_win_rate_pct": round(float(short_win_rate) * 100, 1),
    }


def run_bidirectional_sweep():
    """Run bidirectional backtest and compare with unidirectional."""
    print("=" * 70)
    print("BIDIRECTIONAL CARRY BACKTEST")
    print("=" * 70)

    df = load_all_funding_data()
    print(f"Loaded {len(df):,} records\n")

    configs = [
        {"name": "bidirectional_8borrow", "strategy": "bidirectional_carry",
         "pos_min_ann_rate": 0.08, "neg_min_ann_rate": -0.08,
         "borrow_cost_annual": 0.08},
        {"name": "bidirectional_12borrow", "strategy": "bidirectional_carry",
         "pos_min_ann_rate": 0.08, "neg_min_ann_rate": -0.08,
         "borrow_cost_annual": 0.12},
        {"name": "bidirectional_5borrow", "strategy": "bidirectional_carry",
         "pos_min_ann_rate": 0.08, "neg_min_ann_rate": -0.08,
         "borrow_cost_annual": 0.05},
    ]

    all_results = {}
    out_dir = Path("results/sweep")
    out_dir.mkdir(parents=True, exist_ok=True)

    for cfg_dict in configs:
        name = cfg_dict.pop("name")
        print(f"\n--- {name} ---")

        config = BidirectionalConfig(**cfg_dict)
        bt = BidirectionalBacktest(config)
        start = time.time()
        state = bt.run(df)
        elapsed = time.time() - start

        metrics = compute_metrics(state)
        all_results[name] = metrics

        # Save equity curve
        eq_df = pd.DataFrame(state.equity_history)
        eq_df.to_csv(out_dir / f"equity_{name}.csv", index=False)

        # Save trades
        if state.trade_log:
            pd.DataFrame(state.trade_log).to_csv(out_dir / f"trades_{name}.csv", index=False)

        print(
            f"  Time: {elapsed:.1f}s | Sharpe: {metrics.get('sharpe')} | "
            f"Return: {metrics.get('annual_return_pct')}% | "
            f"MaxDD: {metrics.get('max_drawdown_pct')}% | "
            f"WinRate: {metrics.get('win_rate_pct')}% | "
            f"Trades: {metrics.get('total_trades')} | "
            f"Long: {metrics.get('long_trades')} | "
            f"Short: {metrics.get('short_trades')} | "
            f"Final: ${metrics.get('final_equity', 0):,.0f}"
        )

    # Summary
    print(f"\n{'='*70}")
    print("BIDIRECTIONAL SWEEP RESULTS")
    print(f"{'='*70}")

    summary = pd.DataFrame(all_results).T
    cols = [
        "sharpe", "annual_return_pct", "max_drawdown_pct", "sortino", "calmar",
        "win_rate_pct", "total_trades", "profit_factor", "avg_hold_days",
        "long_trades", "short_trades", "long_win_rate_pct", "short_win_rate_pct",
        "final_equity",
    ]
    available = [c for c in cols if c in summary.columns]
    print(summary[available].to_string())

    summary.to_csv(out_dir / "bidirectional_comparison.csv")
    with open(out_dir / "bidirectional_metrics.json", "w") as f:
        json.dump(all_results, f, indent=2, default=str)

    print(f"\nResults saved to {out_dir}/")
    return all_results


if __name__ == "__main__":
    run_bidirectional_sweep()
