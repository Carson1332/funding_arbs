"""
Parameter sweep backtest with improved strategies.

Strategy research findings:
1. Adaptive Carry — conservative entry, ride autocorrelation
2. Term Structure — requires tighter entry criteria
3. Mean Reversion — aggressive take-profit causes over-trading
4. Cross-Exchange — sparse spread data, costs erode edge
5. Composite — excessive signals lead to over-trading

Improvements implemented:
- Removed take-profit to let winners run in carry trade
- Reduced trading frequency via wider entry thresholds
- Composite combines only the two strongest strategies
- Parameter sweep to identify optimal settings
- Signal-strength-based position sizing
"""
from __future__ import annotations

import json
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import yaml

sys.path.insert(0, str(Path(__file__).resolve().parent))

from backtest.enhanced_engine import (
    compute_funding_features,
    load_all_funding_data,
)


def _load_fee_scenarios(path: str = "config/fees.yaml") -> dict[str, dict[str, Any]]:
    """Load fee scenarios from YAML. Returns dict keyed by scenario name."""
    p = Path(path)
    if not p.exists():
        return {
            "default": {
                "spot_taker_fee": 0.0010,
                "perp_taker_fee": 0.0005,
                "slippage_bps": 1.0,
            }
        }
    with open(p) as f:
        data = yaml.safe_load(f)
    return {
        name: {
            "spot_taker_fee": cfg.get("spot_taker_fee", 0.0010),
            "perp_taker_fee": cfg.get("perp_taker_fee", 0.0005),
            "slippage_bps": cfg.get("slippage_bps", 1.0),
        }
        for name, cfg in data.get("fee_scenarios", {}).items()
    }


@dataclass
class SweepConfig:
    initial_capital: float = 100_000.0
    max_positions: int = 8
    max_position_pct: float = 0.20
    max_drawdown_exit: float = 0.08
    max_single_loss_pct: float = 0.015

    # Fee model
    spot_taker_fee: float = 0.0010
    perp_taker_fee: float = 0.0005
    slippage_bps: float = 1.0

    # Strategy-specific
    strategy: str = "adaptive_carry_v3"

    # Adaptive carry params
    min_ann_rate_entry: float = 0.08
    min_positive_streak: int = 6
    exit_ann_rate: float = 0.02
    exit_momentum_threshold: float = -0.00008
    max_hold_periods: int = 180  # 60 days

    # Term structure params
    ts_slope_entry: float = 0.00003
    ts_long_rate_min: float = 0.04
    ts_slope_exit: float = -0.00005

    # Capital cost
    stablecoin_yield: float = 0.04  # annual opportunity cost on locked collateral


@dataclass
class CarryPosition:
    symbol: str
    exchange: str
    entry_time: datetime
    collateral: float
    entry_cost: float
    funding_collected: float = 0.0
    basis_pnl: float = 0.0
    periods_held: int = 0
    peak_pnl: float = 0.0
    signal_strength: float = 1.0
    direction: str = "long"

    # Price tracking for basis MtM
    entry_spot_price: float = 0.0
    entry_perp_price: float = 0.0
    last_spot_price: float = 0.0
    last_perp_price: float = 0.0

    @property
    def quantity(self) -> float:
        return self.collateral / self.entry_perp_price if self.entry_perp_price > 0 else 0.0

    @property
    def net_pnl(self) -> float:
        return self.funding_collected + self.basis_pnl - self.entry_cost

    @property
    def return_on_collateral(self) -> float:
        return self.net_pnl / self.collateral if self.collateral > 0 else 0

    def update_basis_mtm(self, spot_price: float, perp_price: float) -> float:
        """Compute and add period basis MtM. Returns period PnL."""
        if self.last_spot_price <= 0 or self.last_perp_price <= 0:
            self.last_spot_price = spot_price
            self.last_perp_price = perp_price
            return 0.0
        q = self.quantity
        if self.direction == "long":
            period_pnl = q * ((spot_price - self.last_spot_price) - (perp_price - self.last_perp_price))
        else:
            period_pnl = q * (-(spot_price - self.last_spot_price) + (perp_price - self.last_perp_price))
        self.basis_pnl += period_pnl
        self.last_spot_price = spot_price
        self.last_perp_price = perp_price
        return period_pnl


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


class SweepBacktest:
    def __init__(self, config: SweepConfig):
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
        # Scale by signal strength (0.5 to 1.0)
        scale = 0.5 + 0.5 * min(signal_strength, 1.0)
        return base_size * scale

    def _can_open(self) -> bool:
        return (len(self.state.positions) < self.config.max_positions and
                self.state.available_capital > self.state.equity * 0.05)

    def _open(self, ts, symbol, exchange, rate, signal_strength=1.0,
              spot_price: float = 0.0, perp_price: float = 0.0, direction: str = "long"):
        collateral = self._position_size(signal_strength)
        if collateral < 100:
            return
        cost = self._entry_cost(collateral)
        key = f"{symbol}_{exchange}"
        self.state.positions[key] = CarryPosition(
            symbol=symbol, exchange=exchange, entry_time=ts,
            collateral=collateral, entry_cost=cost,
            signal_strength=signal_strength,
            entry_spot_price=spot_price, entry_perp_price=perp_price,
            last_spot_price=spot_price, last_perp_price=perp_price,
            direction=direction,
        )
        self.state.trade_log.append({
            "timestamp": ts, "symbol": symbol, "action": "OPEN",
            "collateral": collateral, "entry_cost": cost,
            "funding_rate": rate, "signal_strength": signal_strength,
            "spot_price": spot_price, "perp_price": perp_price,
            "direction": direction,
            "equity": self.state.equity,
        })

    def _collect_period_pnl(self, row: pd.Series, pos: CarryPosition) -> None:
        """Update funding, basis MtM, and opportunity cost for one position."""
        rate = row["funding_rate"]
        if pos.direction == "long":
            pos.funding_collected += pos.collateral * rate
        else:
            pos.funding_collected += pos.collateral * (-rate)

        # Basis mark-to-market
        for col_spot, col_perp in [("spot_close", "perp_close")]:
            if col_spot in row.index and col_perp in row.index:
                sp = row[col_spot]
                pp = row[col_perp]
                if pd.notna(sp) and pd.notna(pp) and sp > 0 and pp > 0:
                    pos.update_basis_mtm(float(sp), float(pp))

        # Opportunity cost on locked collateral
        if self.config.stablecoin_yield > 0:
            cost_per_period = pos.collateral * (self.config.stablecoin_yield / 1095)
            pos.funding_collected -= cost_per_period

        pos.periods_held += 1
        pos.peak_pnl = max(pos.peak_pnl, pos.net_pnl)

    def _close(self, ts, key, reason="signal"):
        if key not in self.state.positions:
            return
        pos = self.state.positions[key]
        exit_cost = self._exit_cost(pos.collateral)
        realised = pos.net_pnl - exit_cost
        self.state._cumulative_realised_pnl += realised
        self.state.trade_log.append({
            "timestamp": ts, "symbol": pos.symbol, "action": "CLOSE",
            "reason": reason, "collateral": pos.collateral,
            "funding_collected": pos.funding_collected,
            "basis_pnl": pos.basis_pnl,
            "total_costs": pos.entry_cost + exit_cost,
            "net_pnl": realised,
            "periods_held": pos.periods_held,
            "return_pct": realised / pos.collateral * 100,
            "equity": self.state.equity,
        })
        del self.state.positions[key]

    def run_adaptive_carry(self, df: pd.DataFrame) -> PortfolioState:
        """
        Optimized Adaptive Carry strategy.

        Design choices:
        - No take-profit to let winners run
        - Signal-strength-based position sizing
        - Wider entry threshold (8% annualised minimum)
        - Longer max hold (180 periods = 60 days)
        - Tighter momentum exit criteria
        """
        features = compute_funding_features(df)
        binance = features[features["exchange"] == "binance"].copy()
        timestamps = sorted(binance["timestamp"].unique())

        print(f"Running Adaptive Carry: {len(timestamps)} periods, "
              f"{binance['symbol'].nunique()} symbols")

        for i, ts in enumerate(timestamps):
            snap = binance[binance["timestamp"] == ts]

            # 1. Collect funding + basis MtM + opportunity cost
            for key, pos in list(self.state.positions.items()):
                row = snap[snap["symbol"] == pos.symbol]
                if row.empty:
                    continue
                self._collect_period_pnl(row.iloc[0], pos)

            # 2. Portfolio risk
            if self.state.drawdown > self.config.max_drawdown_exit:
                for key in list(self.state.positions.keys()):
                    self._close(ts, key, "max_drawdown")

            # 3. Exit signals
            for key, pos in list(self.state.positions.items()):
                row = snap[snap["symbol"] == pos.symbol]
                if row.empty:
                    continue
                r = row.iloc[0]

                # Loss limit
                if pos.net_pnl < -self.state.equity * self.config.max_single_loss_pct:
                    self._close(ts, key, "max_loss")
                    continue

                # 7d mean drops below exit threshold
                ann_7d = r["fr_mean_7d"] * 3 * 365
                if pd.notna(r.get("fr_mean_7d")) and ann_7d < self.config.exit_ann_rate:
                    self._close(ts, key, "low_rate")
                    continue

                # Strong negative momentum
                mom_7d = r["fr_momentum_7d"]
                if (
                    pd.notna(r.get("fr_momentum_7d"))
                    and mom_7d < self.config.exit_momentum_threshold
                ):
                    self._close(ts, key, "neg_momentum")
                    continue

                # Max hold
                if pos.periods_held > self.config.max_hold_periods:
                    self._close(ts, key, "max_hold")
                    continue

            # 4. Entry signals
            if self._can_open():
                candidates = []
                for _, r in snap.iterrows():
                    sym = r["symbol"]
                    key = f"{sym}_binance"
                    if key in self.state.positions:
                        continue

                    if pd.isna(r.get("fr_mean_7d")) or pd.isna(r.get("fr_momentum_3d")):
                        continue

                    ann_7d = r["fr_mean_7d"] * 3 * 365
                    if ann_7d < self.config.min_ann_rate_entry:
                        continue

                    if r["fr_momentum_3d"] < 0:
                        continue

                    streak = r["positive_streak"]
                    if (
                        pd.notna(r.get("positive_streak"))
                        and streak < self.config.min_positive_streak
                    ):
                        continue

                    if pd.notna(r.get("ema_crossover")) and r["ema_crossover"] < 0:
                        continue

                    # Signal strength based on rate level and momentum
                    strength = min(ann_7d / 0.20, 1.0)
                    if r["fr_momentum_3d"] > 0.00005:
                        strength *= 1.2

                    score = ann_7d + r["fr_momentum_3d"] * 3000
                    candidates.append((sym, r, score, strength))

                candidates.sort(key=lambda x: x[2], reverse=True)
                for sym, r, score, strength in candidates:
                    if not self._can_open():
                        break
                    spot_price = r.get("spot_close", 0.0)
                    perp_price = r.get("perp_close", 0.0)
                    self._open(ts, sym, "binance", r["funding_rate"], strength,
                               spot_price=spot_price, perp_price=perp_price, direction="long")

            # 5. Record
            self.state.equity_history.append({
                "timestamp": ts, "equity": self.state.equity,
                "positions": len(self.state.positions),
                "drawdown": self.state.drawdown,
                "locked": self.state.locked_collateral,
            })

            if (i + 1) % 500 == 0:
                print(f"  Period {i+1}/{len(timestamps)}: equity=${self.state.equity:,.0f}, "
                      f"pos={len(self.state.positions)}, dd={self.state.drawdown:.2%}")

        return self.state

    def run_carry_plus_ts(self, df: pd.DataFrame) -> PortfolioState:
        """
        Combined Carry + Term Structure strategy.

        Entry requires BOTH:
        - Carry signal: 7d mean > 6% annualised, positive momentum
        - Term structure: slope > 0 (short > long)

        More selective than either strategy in isolation.
        """
        features = compute_funding_features(df)
        binance = features[features["exchange"] == "binance"].copy()
        timestamps = sorted(binance["timestamp"].unique())

        print(f"Running Carry+TS Combined: {len(timestamps)} periods, "
              f"{binance['symbol'].nunique()} symbols")

        for i, ts in enumerate(timestamps):
            snap = binance[binance["timestamp"] == ts]

            # 1. Collect funding + basis MtM + opportunity cost
            for key, pos in list(self.state.positions.items()):
                row = snap[snap["symbol"] == pos.symbol]
                if row.empty:
                    continue
                self._collect_period_pnl(row.iloc[0], pos)

            # 2. Portfolio risk
            if self.state.drawdown > self.config.max_drawdown_exit:
                for key in list(self.state.positions.keys()):
                    self._close(ts, key, "max_drawdown")

            # 3. Exit: either carry OR term structure exit triggers
            for key, pos in list(self.state.positions.items()):
                row = snap[snap["symbol"] == pos.symbol]
                if row.empty:
                    continue
                r = row.iloc[0]

                if pos.net_pnl < -self.state.equity * self.config.max_single_loss_pct:
                    self._close(ts, key, "max_loss")
                    continue

                exit_signals = 0

                # Carry exit
                if pd.notna(r.get("fr_mean_7d")) and r["fr_mean_7d"] * 3 * 365 < 0.02:
                    exit_signals += 1
                if pd.notna(r.get("fr_momentum_7d")) and r["fr_momentum_7d"] < -0.00006:
                    exit_signals += 1

                # TS exit
                if pd.notna(r.get("ts_slope")) and r["ts_slope"] < -0.00003:
                    exit_signals += 1
                if pd.notna(r.get("fr_mean_30d")) and r["fr_mean_30d"] < 0:
                    exit_signals += 1

                # Rate negative
                if r["funding_rate"] < -0.0002:
                    exit_signals += 2

                # Max hold
                if pos.periods_held > 150 and pos.return_on_collateral < 0.005:
                    exit_signals += 1

                if exit_signals >= 2:
                    self._close(ts, key, f"combined_exit_{exit_signals}")

            # 4. Entry: require BOTH carry and TS signals
            if self._can_open():
                candidates = []
                for _, r in snap.iterrows():
                    sym = r["symbol"]
                    key = f"{sym}_binance"
                    if key in self.state.positions:
                        continue

                    # Carry conditions
                    if pd.isna(r.get("fr_mean_7d")) or pd.isna(r.get("ts_slope")):
                        continue

                    ann_7d = r["fr_mean_7d"] * 3 * 365
                    if ann_7d < self.config.min_ann_rate_entry:
                        continue

                    if pd.notna(r.get("fr_momentum_3d")) and r["fr_momentum_3d"] < 0:
                        continue

                    if r["funding_rate"] <= 0:
                        continue

                    # Term structure conditions
                    if r["ts_slope"] <= 0:
                        continue

                    if pd.notna(r.get("fr_mean_30d")) and r["fr_mean_30d"] * 3 * 365 < 0.03:
                        continue

                    # Positive streak
                    if pd.notna(r.get("positive_streak")) and r["positive_streak"] < 3:
                        continue

                    # Score: weighted combination
                    carry_score = ann_7d
                    ts_score = r["ts_slope"] * 10000
                    strength = min((carry_score + ts_score) / 0.30, 1.0)
                    score = carry_score * 0.6 + ts_score * 0.4

                    candidates.append((sym, r, score, strength))

                candidates.sort(key=lambda x: x[2], reverse=True)
                for sym, r, score, strength in candidates:
                    if not self._can_open():
                        break
                    spot_price = r.get("spot_close", 0.0)
                    perp_price = r.get("perp_close", 0.0)
                    self._open(ts, sym, "binance", r["funding_rate"], strength,
                               spot_price=spot_price, perp_price=perp_price, direction="long")

            # 5. Record
            self.state.equity_history.append({
                "timestamp": ts, "equity": self.state.equity,
                "positions": len(self.state.positions),
                "drawdown": self.state.drawdown,
            })

            if (i + 1) % 500 == 0:
                print(f"  Period {i+1}/{len(timestamps)}: equity=${self.state.equity:,.0f}, "
                      f"pos={len(self.state.positions)}, dd={self.state.drawdown:.2%}")

        return self.state

    def run(self, df: pd.DataFrame) -> PortfolioState:
        strategies = {
            "adaptive_carry": self.run_adaptive_carry,
            "carry_plus_ts": self.run_carry_plus_ts,
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

    # Additional stats
    avg_periods_held = 0
    if not trade_df.empty and "periods_held" in trade_df.columns:
        closes = trade_df[trade_df["action"] == "CLOSE"]
        if len(closes) > 0:
            avg_periods_held = closes["periods_held"].mean()

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
    }


def run_parameter_sweep():
    """Sweep key parameters to find optimal settings."""
    print("=" * 70)
    print("PARAMETER SWEEP")
    print("=" * 70)

    df = load_all_funding_data()
    print(f"Loaded {len(df):,} records\n")

    # Sweep configurations
    configs = [
        # Adaptive Carry variants
        {"name": "carry_strict", "strategy": "adaptive_carry",
         "min_ann_rate_entry": 0.10, "min_positive_streak": 9,
         "exit_ann_rate": 0.03, "max_hold_periods": 180},
        {"name": "carry_moderate", "strategy": "adaptive_carry",
         "min_ann_rate_entry": 0.08, "min_positive_streak": 6,
         "exit_ann_rate": 0.02, "max_hold_periods": 180},
        {"name": "carry_relaxed", "strategy": "adaptive_carry",
         "min_ann_rate_entry": 0.06, "min_positive_streak": 3,
         "exit_ann_rate": 0.01, "max_hold_periods": 270},

        # Carry + TS variants
        {"name": "carry_ts_strict", "strategy": "carry_plus_ts",
         "min_ann_rate_entry": 0.08},
        {"name": "carry_ts_moderate", "strategy": "carry_plus_ts",
         "min_ann_rate_entry": 0.06},

        # Different position sizing
        {"name": "carry_concentrated", "strategy": "adaptive_carry",
         "max_positions": 5, "max_position_pct": 0.30,
         "min_ann_rate_entry": 0.10, "min_positive_streak": 9},
        {"name": "carry_diversified", "strategy": "adaptive_carry",
         "max_positions": 12, "max_position_pct": 0.12,
         "min_ann_rate_entry": 0.08, "min_positive_streak": 6},
    ]

    fee_scenarios = _load_fee_scenarios()
    all_results = {}
    out_dir = Path("results/sweep")
    out_dir.mkdir(exist_ok=True)

    for cfg_dict in configs:
        name = cfg_dict.pop("name")

        for fee_name, fee_vals in fee_scenarios.items():
            run_name = f"{name}_{fee_name}" if len(fee_scenarios) > 1 else name
            print(f"\n--- {run_name} ---")

            merged_cfg = {**cfg_dict, **fee_vals}
            config = SweepConfig(**merged_cfg)
            bt = SweepBacktest(config)
            start = time.time()
            state = bt.run(df)
            elapsed = time.time() - start

            metrics = compute_metrics(state)
            all_results[run_name] = metrics

            # Save equity curve
            eq_df = pd.DataFrame(state.equity_history)
            eq_df.to_csv(out_dir / f"equity_{run_name}.csv", index=False)

            # Save trades
            if state.trade_log:
                pd.DataFrame(state.trade_log).to_csv(out_dir / f"trades_{run_name}.csv", index=False)

            print(f"  Time: {elapsed:.1f}s | Sharpe: {metrics.get('sharpe')} | "
                  f"Return: {metrics.get('annual_return_pct')}% | "
                  f"MaxDD: {metrics.get('max_drawdown_pct')}% | "
                  f"WinRate: {metrics.get('win_rate_pct')}% | "
                  f"Trades: {metrics.get('total_trades')} | "
                  f"PF: {metrics.get('profit_factor')} | "
                  f"Final: ${metrics.get('final_equity', 0):,.0f}")

    # Summary
    print(f"\n{'='*70}")
    print("PARAMETER SWEEP RESULTS")
    print(f"{'='*70}")

    summary = pd.DataFrame(all_results).T
    cols = ["sharpe", "annual_return_pct", "max_drawdown_pct", "sortino", "calmar",
            "win_rate_pct", "total_trades", "profit_factor", "avg_hold_days", "final_equity"]
    available = [c for c in cols if c in summary.columns]
    print(summary[available].to_string())

    summary.to_csv(out_dir / "sweep_comparison.csv")
    with open(out_dir / "all_metrics.json", "w") as f:
        json.dump(all_results, f, indent=2, default=str)

    print(f"\nResults saved to {out_dir}/")
    return all_results


if __name__ == "__main__":
    run_parameter_sweep()
