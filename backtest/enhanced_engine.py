"""
Enhanced Funding Rate Arbitrage Backtest Engine.

Accounting Model for Delta-Neutral Carry:
In a funding rate arbitrage, the trader:
  1. Buys spot (or deposits as collateral) — capital is locked, not spent
  2. Shorts perpetual using the spot as collateral
  3. Every 8 hours, collects funding when the rate is positive (short receives)
  4. On exit, recovers locked capital plus/minus basis drift and funding collected

P&L per period = funding payment - basis change cost.
Since the position is delta-neutral, basis change is near zero over short horizons.
The main costs are entry/exit fees and slippage (one-time) and negative funding (ongoing).

Capital allocation: each position locks capital as collateral.
Available capital = total_equity - sum(locked_collateral).

Strategies:
1. Adaptive Carry — momentum-weighted entry with autocorrelation exploitation
2. Cross-Exchange Spread — exploit funding rate divergence between exchanges
3. Term Structure — enter on steep curve, exit on inversion
4. Mean Reversion — ride funding spikes, exit before reversion
5. Composite — weighted ensemble of all signals
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

# ============================================================================
# DATA LOADING
# ============================================================================

def load_all_funding_data(merge_prices: bool = True) -> pd.DataFrame:
    """Load all cached funding rate parquet files.

    Optionally merges spot/perp close prices for basis MtM if OHLCV cache exists.

    Data alignment fixes applied:
    1. Floor all timestamps to the nearest second to eliminate sub-second jitter
       from exchange APIs (e.g. Binance returns 00:00:00.001 instead of 00:00:00).
    2. For native 8h symbols, snap to canonical hours [0, 8, 16] via floor.
    3. For sub-8h symbols (1h/4h), aggregate into 8h buckets with
       closed='left', label='left' so that bucket [00:00, 08:00) -> 00:00,
       preserving total funding income and aligning with the canonical grid.
    """
    cache_dir = Path(__file__).resolve().parent.parent / "data" / "cache" / "funding_rates"
    frames = []
    for f in cache_dir.glob("*.parquet"):
        try:
            df = pd.read_parquet(f)
            frames.append(df)
        except Exception:
            pass
    if not frames:
        raise RuntimeError("No funding rate data found in cache")
    combined = pd.concat(frames, ignore_index=True)
    combined["timestamp"] = pd.to_datetime(combined["timestamp"], utc=True)

    # FIX 1: Floor to nearest second to remove sub-second API jitter.
    # Exchange APIs (especially Binance) return timestamps with +/-1-10ms offsets
    # (e.g. 00:00:00.001 instead of 00:00:00). Without flooring, two symbols
    # with the "same" 8h timestamp appear as different timestamps, fragmenting
    # the backtest's timestamp grid (3357 unique instead of ~2522).
    combined["timestamp"] = combined["timestamp"].dt.floor("s")

    # Standardise all symbols to 8h funding intervals.
    # Some symbols (e.g. TIA, WIF, AXS) arrive with 1h/4h granularity from ccxt.
    # We detect the native interval per (exchange, symbol) and aggregate
    # fine-grained rates into 8h buckets so total funding income is conserved.
    combined = combined.sort_values(["exchange", "symbol", "timestamp"])
    combined = combined.drop_duplicates(subset=["exchange", "symbol", "timestamp"], keep="first")
    combined["dt_hours"] = (
        combined.groupby(["exchange", "symbol"])["timestamp"]
        .diff().dt.total_seconds() / 3600
    )

    def _to_8h(g: pd.DataFrame, sym: str, exc: str) -> pd.DataFrame:
        median_dt = g["dt_hours"].median()
        if pd.isna(median_dt) or median_dt >= 7.5:
            # Native 8h -- snap to canonical settlement hours.
            return g[g["timestamp"].dt.hour.isin([0, 8, 16])]
        # Fine-grained -> 8h bucket, funding_rate summed (linear in rate).
        # closed='left', label='left': bucket [00:00, 08:00) labelled 00:00
        # so output timestamps align with the canonical 0/8/16 grid.
        g = g.set_index("timestamp")
        agg = g.resample(
            "8h", origin="start_day", closed="left", label="left"
        ).agg({
            "funding_rate": "sum",
        }).dropna(subset=["funding_rate"]).reset_index()
        agg["symbol"] = sym
        agg["exchange"] = exc
        return agg

    _results: list[pd.DataFrame] = []
    for (exc, sym), g in combined.groupby(["exchange", "symbol"]):
        _results.append(_to_8h(g, sym, exc))
    combined = pd.concat(_results, ignore_index=True) if _results else pd.DataFrame()

    # Drop the helper column
    if "dt_hours" in combined.columns:
        combined = combined.drop(columns=["dt_hours"])

    if merge_prices:
        try:
            from data.spot_prices import load_all_basis_data
            basis_df = load_all_basis_data()
            if not basis_df.empty:
                basis_df["timestamp"] = pd.to_datetime(basis_df["timestamp"], utc=True)
                basis_df["timestamp"] = basis_df["timestamp"].dt.floor("s")
                combined = combined.merge(
                    basis_df[["timestamp", "symbol", "exchange", "spot_close", "perp_close", "basis"]],
                    on=["timestamp", "symbol", "exchange"],
                    how="left",
                )
        except Exception:
            pass

    combined = combined.sort_values(["symbol", "exchange", "timestamp"]).reset_index(drop=True)
    return combined


# ============================================================================
# FEATURE ENGINEERING
# ============================================================================

def compute_funding_features(df: pd.DataFrame) -> pd.DataFrame:
    """Compute rich feature set from raw funding rate data.

    Uses lag-1 funding rate for all signal calculations to avoid look-ahead bias:
    the signal at time t is based on the realised funding rate up to t-1,
    so the decision to enter at t cannot exploit the (as-yet-unknown) rate at t.
    """
    results = []
    for (sym, exc), g in df.groupby(["symbol", "exchange"]):
        g = g.copy().sort_values("timestamp")
        # Lagged rate for signals (t uses t-1 rate)
        g["funding_rate_lag1"] = g["funding_rate"].shift(1)
        fr = g["funding_rate_lag1"]

        # Rolling statistics
        for w, label in [(9, "3d"), (21, "7d"), (63, "21d"), (90, "30d")]:
            g[f"fr_mean_{label}"] = fr.rolling(w, min_periods=max(3, w // 3)).mean()
            g[f"fr_std_{label}"] = fr.rolling(w, min_periods=max(3, w // 3)).std()

        # Z-scores
        g["zscore_7d"] = (fr - g["fr_mean_7d"]) / g["fr_std_7d"].replace(0, np.nan)
        g["zscore_30d"] = (fr - g["fr_mean_30d"]) / g["fr_std_30d"].replace(0, np.nan)

        # Annualised
        g["ann_rate"] = fr * 3 * 365

        # Momentum
        g["fr_momentum_3d"] = g["fr_mean_3d"].diff(3)
        g["fr_momentum_7d"] = g["fr_mean_7d"].diff(9)

        # Term structure slope
        g["ts_slope"] = g["fr_mean_3d"] - g["fr_mean_30d"]

        # EMA
        g["fr_ema_fast"] = fr.ewm(span=6, min_periods=3).mean()
        g["fr_ema_slow"] = fr.ewm(span=30, min_periods=10).mean()
        g["ema_crossover"] = g["fr_ema_fast"] - g["fr_ema_slow"]

        # Cumulative funding
        g["cum_funding_7d"] = fr.rolling(21, min_periods=7).sum()

        # Positive streak
        g["is_positive"] = (fr > 0).astype(int)
        streaks = g["is_positive"].groupby(
            (g["is_positive"] != g["is_positive"].shift()).cumsum()
        ).cumsum()
        g["positive_streak"] = streaks * g["is_positive"]

        # Negative streak (mirror of positive streak)
        g["is_negative"] = (fr < 0).astype(int)
        neg_streaks = g["is_negative"].groupby(
            (g["is_negative"] != g["is_negative"].shift()).cumsum()
        ).cumsum()
        g["negative_streak"] = neg_streaks * g["is_negative"]

        # Volatility ratio
        g["fr_vol_7d"] = fr.rolling(21, min_periods=7).std()
        g["fr_vol_30d"] = fr.rolling(90, min_periods=30).std()
        g["fr_vol_ratio"] = g["fr_vol_7d"] / g["fr_vol_30d"].replace(0, np.nan)

        results.append(g)

    return pd.concat(results, ignore_index=True)


def compute_cross_exchange_features(df: pd.DataFrame) -> pd.DataFrame:
    """Compute cross-exchange spread features."""
    pivot = df.pivot_table(
        index=["timestamp", "symbol"],
        columns="exchange",
        values="funding_rate",
        aggfunc="first",
    ).reset_index()

    exchanges = [c for c in pivot.columns if c not in ("timestamp", "symbol")]
    if len(exchanges) < 2:
        return pd.DataFrame()

    spreads = []
    for i, ea in enumerate(exchanges):
        for eb in exchanges[i + 1:]:
            for long_exc, short_exc in [(ea, eb), (eb, ea)]:
                temp = pivot[["timestamp", "symbol"]].copy()
                temp["exchange_long"] = long_exc
                temp["exchange_short"] = short_exc
                temp["rate_long"] = pivot[long_exc]  # we pay this
                temp["rate_short"] = pivot[short_exc]  # we receive this
                temp["spread"] = pivot[short_exc] - pivot[long_exc]
                temp["spread_ann"] = temp["spread"] * 3 * 365
                temp = temp.dropna(subset=["spread"])
                spreads.append(temp)

    if not spreads:
        return pd.DataFrame()
    return pd.concat(spreads, ignore_index=True)


# ============================================================================
# PORTFOLIO MODEL — CORRECT CARRY TRADE ACCOUNTING
# ============================================================================

@dataclass
class StrategyConfig:
    initial_capital: float = 100_000.0
    max_positions: int = 8
    max_position_pct: float = 0.20  # max 20% of equity per position
    max_drawdown_exit: float = 0.08
    max_single_loss_pct: float = 0.015

    # Fee model (realistic for VIP-0 tier)
    spot_taker_fee: float = 0.0010    # 0.10%
    perp_taker_fee: float = 0.0005    # 0.05%
    slippage_bps: float = 1.0         # 1 bp per leg

    strategy: str = "composite"


@dataclass
class CarryPosition:
    """
    A delta-neutral carry position.

    Capital is LOCKED as collateral, not spent.
    P&L = cumulative funding payments + basis mark-to-market - trading costs.
    """
    symbol: str
    exchange: str
    entry_time: datetime
    collateral: float       # capital locked as collateral
    entry_cost: float       # one-time entry fee
    funding_collected: float = 0.0
    basis_pnl: float = 0.0  # mark-to-market from spot-perp price drift
    periods_held: int = 0
    peak_pnl: float = 0.0

    # Price tracking for basis MtM
    entry_spot_price: float = 0.0
    entry_perp_price: float = 0.0
    last_spot_price: float = 0.0
    last_perp_price: float = 0.0
    direction: str = "long"  # "long" = long spot / short perp; "short" = short spot / long perp

    # For cross-exchange
    exchange_short: str = ""
    exchange_long: str = ""
    is_cross_exchange: bool = False

    @property
    def quantity(self) -> float:
        """Notional quantity (base currency units)."""
        return self.collateral / self.entry_perp_price if self.entry_perp_price > 0 else 0.0

    @property
    def net_pnl(self) -> float:
        """Net P&L = funding + basis MtM - entry cost."""
        return self.funding_collected + self.basis_pnl - self.entry_cost

    @property
    def return_on_collateral(self) -> float:
        """Return as fraction of collateral."""
        return self.net_pnl / self.collateral if self.collateral > 0 else 0

    def update_basis_mtm(self, spot_price: float, perp_price: float) -> float:
        """Compute and add period basis MtM. Returns period PnL."""
        if self.last_spot_price <= 0 or self.last_perp_price <= 0:
            self.last_spot_price = spot_price
            self.last_perp_price = perp_price
            return 0.0

        q = self.quantity
        if self.direction == "long":
            # long spot + short perp
            period_pnl = q * ((spot_price - self.last_spot_price) - (perp_price - self.last_perp_price))
        else:
            # short spot + long perp
            period_pnl = q * (-(spot_price - self.last_spot_price) + (perp_price - self.last_perp_price))

        self.basis_pnl += period_pnl
        self.last_spot_price = spot_price
        self.last_perp_price = perp_price
        return period_pnl


@dataclass
class PortfolioState:
    """Portfolio state with correct carry trade accounting."""
    initial_capital: float = 100_000.0
    positions: dict = field(default_factory=dict)
    equity_history: list = field(default_factory=list)
    trade_log: list = field(default_factory=list)
    _cumulative_realised_pnl: float = 0.0

    @property
    def locked_collateral(self) -> float:
        """Total capital locked in positions."""
        return sum(p.collateral for p in self.positions.values())

    @property
    def unrealised_pnl(self) -> float:
        """Sum of unrealised P&L across positions."""
        return sum(p.net_pnl for p in self.positions.values())

    @property
    def equity(self) -> float:
        """Total equity = initial + realised + unrealised."""
        return self.initial_capital + self._cumulative_realised_pnl + self.unrealised_pnl

    @property
    def available_capital(self) -> float:
        """Capital available for new positions."""
        return self.equity - self.locked_collateral

    @property
    def drawdown(self) -> float:
        if not self.equity_history:
            return 0.0
        peak = max(e["equity"] for e in self.equity_history)
        return max(0, (peak - self.equity) / peak) if peak > 0 else 0.0


# ============================================================================
# BACKTEST ENGINE
# ============================================================================

class EnhancedBacktest:
    """Enhanced backtest with carry trade accounting."""

    def __init__(self, config: StrategyConfig):
        self.config = config
        self.state = PortfolioState(initial_capital=config.initial_capital)

    def _round_trip_cost(self, collateral: float) -> float:
        """Total entry + exit cost for a position."""
        # Entry: buy spot (taker) + short perp (taker) + slippage both legs
        # Exit: sell spot (taker) + close perp (taker) + slippage both legs
        spot_cost = collateral * self.config.spot_taker_fee * 2  # entry + exit
        perp_cost = collateral * self.config.perp_taker_fee * 2
        slippage = collateral * (self.config.slippage_bps / 10000) * 4  # 4 legs total
        return spot_cost + perp_cost + slippage

    def _entry_cost_only(self, collateral: float) -> float:
        """Entry cost only (half of round trip)."""
        return self._round_trip_cost(collateral) / 2

    def _exit_cost_only(self, collateral: float) -> float:
        """Exit cost only (half of round trip)."""
        return self._round_trip_cost(collateral) / 2

    def _position_size(self) -> float:
        """Compute position size."""
        max_by_equity = self.state.equity * self.config.max_position_pct
        available = self.state.available_capital * 0.9  # keep 10% buffer
        return max(0, min(max_by_equity, available))

    def _can_open(self) -> bool:
        return (len(self.state.positions) < self.config.max_positions and
                self.state.available_capital > self.state.equity * 0.05)

    def _open_position(self, ts, symbol, exchange, rate,
                       exchange_short="", exchange_long="", is_cross=False,
                       spot_price: float = 0.0, perp_price: float = 0.0,
                       direction: str = "long"):
        """Open a carry position."""
        collateral = self._position_size()
        if collateral < 100:
            return

        entry_cost = self._entry_cost_only(collateral)

        if is_cross:
            key = f"{symbol}_{exchange_short}_{exchange_long}"
        else:
            key = f"{symbol}_{exchange}"

        pos = CarryPosition(
            symbol=symbol,
            exchange=exchange,
            entry_time=ts,
            collateral=collateral,
            entry_cost=entry_cost,
            entry_spot_price=spot_price,
            entry_perp_price=perp_price,
            last_spot_price=spot_price,
            last_perp_price=perp_price,
            direction=direction,
            exchange_short=exchange_short,
            exchange_long=exchange_long,
            is_cross_exchange=is_cross,
        )
        self.state.positions[key] = pos

        self.state.trade_log.append({
            "timestamp": ts, "symbol": symbol, "action": "OPEN",
            "exchange": exchange, "collateral": collateral,
            "entry_cost": entry_cost, "funding_rate": rate,
            "spot_price": spot_price, "perp_price": perp_price,
            "direction": direction,
            "equity": self.state.equity,
        })

    def _close_position(self, ts, key, reason="signal"):
        """Close a carry position and realise P&L."""
        if key not in self.state.positions:
            return

        pos = self.state.positions[key]
        exit_cost = self._exit_cost_only(pos.collateral)
        realised = pos.net_pnl - exit_cost  # funding - entry_cost - exit_cost

        self.state._cumulative_realised_pnl += realised

        self.state.trade_log.append({
            "timestamp": ts, "symbol": pos.symbol, "action": "CLOSE",
            "exchange": pos.exchange, "reason": reason,
            "collateral": pos.collateral,
            "funding_collected": pos.funding_collected,
            "total_costs": pos.entry_cost + exit_cost,
            "net_pnl": realised,
            "periods_held": pos.periods_held,
            "return_pct": realised / pos.collateral * 100 if pos.collateral > 0 else 0,
            "equity": self.state.equity,
        })

        del self.state.positions[key]

    # ========================================================================
    # STRATEGY 1: ADAPTIVE CARRY
    # ========================================================================
    def run_adaptive_carry(self, df: pd.DataFrame) -> PortfolioState:
        """
        Adaptive Carry: exploit funding rate autocorrelation.

        Entry: 7d mean > 8% ann AND momentum positive AND positive streak >= 6
        Exit: 7d mean < 3% ann OR momentum strongly negative OR rate < 0 for 3+ periods
        """
        features = compute_funding_features(df)
        binance = features[features["exchange"] == "binance"].copy()
        timestamps = sorted(binance["timestamp"].unique())

        print(f"Running Adaptive Carry: {len(timestamps)} periods, "
              f"{binance['symbol'].nunique()} symbols")

        for i, ts in enumerate(timestamps):
            snap = binance[binance["timestamp"] == ts]
            self._step_carry(ts, snap, i, len(timestamps),
                             entry_fn=self._adaptive_carry_entry,
                             exit_fn=self._adaptive_carry_exit)

        return self.state

    def _adaptive_carry_entry(self, r) -> float:
        """Score entry for adaptive carry. Returns score > 0 for entry."""
        if pd.isna(r.get("fr_mean_7d")) or pd.isna(r.get("fr_momentum_3d")):
            return 0.0

        ann_7d = r["fr_mean_7d"] * 3 * 365
        if ann_7d < 0.08:
            return 0.0

        if r["fr_momentum_3d"] < 0:
            return 0.0

        if pd.notna(r.get("positive_streak")) and r["positive_streak"] < 6:
            return 0.0

        if pd.notna(r.get("ema_crossover")) and r["ema_crossover"] < 0:
            return 0.0

        # Score: weighted by annualised rate and momentum
        score = ann_7d * 0.6 + r["fr_momentum_3d"] * 5000 * 0.4
        return max(0, score)

    def _adaptive_carry_exit(self, pos, r) -> bool:
        """Check exit for adaptive carry."""
        if pd.notna(r.get("fr_mean_7d")) and r["fr_mean_7d"] * 3 * 365 < 0.03:
            return True
        if pd.notna(r.get("fr_momentum_7d")) and r["fr_momentum_7d"] < -0.00008:
            return True
        if pos.periods_held > 120 and pos.return_on_collateral < 0.005:
            return True
        return False

    # ========================================================================
    # STRATEGY 2: TERM STRUCTURE
    # ========================================================================
    def run_term_structure(self, df: pd.DataFrame) -> PortfolioState:
        """
        Term Structure: enter on steep curve, exit on inversion.
        """
        features = compute_funding_features(df)
        binance = features[features["exchange"] == "binance"].copy()
        timestamps = sorted(binance["timestamp"].unique())

        print(f"Running Term Structure: {len(timestamps)} periods")

        for i, ts in enumerate(timestamps):
            snap = binance[binance["timestamp"] == ts]
            self._step_carry(ts, snap, i, len(timestamps),
                             entry_fn=self._ts_entry,
                             exit_fn=self._ts_exit)

        return self.state

    def _ts_entry(self, r) -> float:
        if pd.isna(r.get("ts_slope")) or pd.isna(r.get("fr_mean_30d")):
            return 0.0
        if r["ts_slope"] < 0.00003:
            return 0.0
        if r["fr_mean_30d"] * 3 * 365 < 0.04:
            return 0.0
        if r["funding_rate"] <= 0:
            return 0.0
        return r["ts_slope"] * 10000 + r["fr_mean_30d"] * 3 * 365

    def _ts_exit(self, pos, r) -> bool:
        if pd.notna(r.get("ts_slope")) and r["ts_slope"] < -0.00005:
            return True
        if pd.notna(r.get("fr_mean_30d")) and r["fr_mean_30d"] < 0:
            return True
        if pos.periods_held > 90 and pos.return_on_collateral < 0.003:
            return True
        return False

    # ========================================================================
    # STRATEGY 3: MEAN REVERSION
    # ========================================================================
    def run_mean_reversion(self, df: pd.DataFrame) -> PortfolioState:
        """
        Mean Reversion: ride funding spikes, exit before reversion.
        """
        features = compute_funding_features(df)
        binance = features[features["exchange"] == "binance"].copy()
        timestamps = sorted(binance["timestamp"].unique())

        print(f"Running Mean Reversion: {len(timestamps)} periods")

        for i, ts in enumerate(timestamps):
            snap = binance[binance["timestamp"] == ts]
            self._step_carry(ts, snap, i, len(timestamps),
                             entry_fn=self._mr_entry,
                             exit_fn=self._mr_exit)

        return self.state

    def _mr_entry(self, r) -> float:
        if pd.isna(r.get("zscore_7d")) or pd.isna(r.get("fr_momentum_3d")):
            return 0.0
        if r["zscore_7d"] < 0.8 or r["zscore_7d"] > 3.0:
            return 0.0
        if r["fr_momentum_3d"] <= 0:
            return 0.0
        if r["funding_rate"] * 3 * 365 < 0.05:
            return 0.0
        return r["zscore_7d"] * (r["funding_rate"] * 3 * 365)

    def _mr_exit(self, pos, r) -> bool:
        if pd.notna(r.get("zscore_7d")) and r["zscore_7d"] < -0.3:
            return True
        if r["funding_rate"] < -0.0001:
            return True
        # Take profit at 1.5% of collateral
        if pos.return_on_collateral > 0.015:
            return True
        if pos.periods_held > 45:
            return True
        return False

    # ========================================================================
    # STRATEGY 4: CROSS-EXCHANGE SPREAD
    # ========================================================================
    def run_cross_exchange(self, df: pd.DataFrame) -> PortfolioState:
        """Cross-exchange funding spread arbitrage."""
        spreads = compute_cross_exchange_features(df)
        if spreads.empty:
            print("No cross-exchange spread data")
            return self.state

        # Compute rolling spread features
        spread_features = []
        for (sym, el, es), g in spreads.groupby(["symbol", "exchange_long", "exchange_short"]):
            g = g.copy().sort_values("timestamp")
            g["spread_mean_7d"] = g["spread"].rolling(21, min_periods=5).mean()
            g["spread_std_7d"] = g["spread"].rolling(21, min_periods=5).std()
            g["spread_momentum"] = g["spread_mean_7d"].diff(3)
            spread_features.append(g)

        if not spread_features:
            return self.state

        sf = pd.concat(spread_features, ignore_index=True)
        timestamps = sorted(sf["timestamp"].unique())
        print(f"Running Cross-Exchange Spread: {len(timestamps)} periods")

        for i, ts in enumerate(timestamps):
            snap = sf[sf["timestamp"] == ts]

            # Collect funding on existing positions
            for key, pos in list(self.state.positions.items()):
                row = snap[
                    (snap["symbol"] == pos.symbol) &
                    (snap["exchange_short"] == pos.exchange_short) &
                    (snap["exchange_long"] == pos.exchange_long)
                ]
                if row.empty:
                    continue
                spread = row.iloc[0]["spread"]
                payment = pos.collateral * spread
                pos.funding_collected += payment
                pos.periods_held += 1
                pos.peak_pnl = max(pos.peak_pnl, pos.net_pnl)

            # Risk checks
            if self.state.drawdown > self.config.max_drawdown_exit:
                for key in list(self.state.positions.keys()):
                    self._close_position(ts, key, "max_drawdown")

            # Exit signals
            for key, pos in list(self.state.positions.items()):
                row = snap[
                    (snap["symbol"] == pos.symbol) &
                    (snap["exchange_short"] == pos.exchange_short) &
                    (snap["exchange_long"] == pos.exchange_long)
                ]
                if row.empty:
                    self._close_position(ts, key, "no_data")
                    continue

                r = row.iloc[0]
                if r["spread"] < -0.00005:
                    self._close_position(ts, key, "negative_spread")
                elif pos.net_pnl < -self.state.equity * self.config.max_single_loss_pct:
                    self._close_position(ts, key, "max_loss")
                elif pos.periods_held > 60:
                    self._close_position(ts, key, "max_hold")

            # Entry signals
            if self._can_open():
                profitable = snap[snap["spread_ann"] > 0.15].copy()
                if not profitable.empty:
                    # Also require positive momentum
                    if "spread_momentum" in profitable.columns:
                        profitable = profitable[profitable["spread_momentum"] > 0]
                    profitable = profitable.sort_values("spread_ann", ascending=False)

                    for _, r in profitable.iterrows():
                        if not self._can_open():
                            break
                        key = f"{r['symbol']}_{r['exchange_short']}_{r['exchange_long']}"
                        if key in self.state.positions:
                            continue
                        self._open_position(
                            ts, r["symbol"], r["exchange_short"],
                            r["spread"],
                            exchange_short=r["exchange_short"],
                            exchange_long=r["exchange_long"],
                            is_cross=True,
                        )

            # Record equity
            self.state.equity_history.append({
                "timestamp": ts, "equity": self.state.equity,
                "positions": len(self.state.positions),
                "drawdown": self.state.drawdown,
            })

            if (i + 1) % 200 == 0:
                print(f"  Period {i+1}/{len(timestamps)}: equity=${self.state.equity:,.0f}, "
                      f"pos={len(self.state.positions)}, dd={self.state.drawdown:.2%}")

        return self.state

    # ========================================================================
    # STRATEGY 5: COMPOSITE
    # ========================================================================
    def run_composite(self, df: pd.DataFrame) -> PortfolioState:
        """
        Composite: weighted ensemble of all signals.
        """
        features = compute_funding_features(df)
        binance = features[features["exchange"] == "binance"].copy()
        timestamps = sorted(binance["timestamp"].unique())

        print(f"Running Composite: {len(timestamps)} periods, "
              f"{binance['symbol'].nunique()} symbols")

        for i, ts in enumerate(timestamps):
            snap = binance[binance["timestamp"] == ts]
            self._step_carry(ts, snap, i, len(timestamps),
                             entry_fn=self._composite_entry,
                             exit_fn=self._composite_exit)

        return self.state

    def _composite_entry(self, r) -> float:
        """Composite entry scoring."""
        if pd.isna(r.get("fr_mean_7d")):
            return 0.0
        if r["funding_rate"] <= 0:
            return 0.0

        score = 0.0

        # 1. Carry component (40%)
        ann_7d = r["fr_mean_7d"] * 3 * 365
        if ann_7d > 0.06:
            carry = min(ann_7d / 0.25, 1.0)
            if pd.notna(r.get("fr_momentum_3d")) and r["fr_momentum_3d"] > 0:
                carry *= 1.2
            if pd.notna(r.get("ema_crossover")) and r["ema_crossover"] > 0:
                carry *= 1.15
            score += 0.40 * carry

        # 2. Term structure (25%)
        if pd.notna(r.get("ts_slope")) and r["ts_slope"] > 0.00001:
            ts_score = min(r["ts_slope"] / 0.0002, 1.0)
            score += 0.25 * ts_score

        # 3. Mean reversion timing (20%)
        if pd.notna(r.get("zscore_7d")):
            if 0.5 < r["zscore_7d"] < 2.5:
                mr = 1.0 - abs(r["zscore_7d"] - 1.5) / 2.0
                if pd.notna(r.get("fr_momentum_3d")) and r["fr_momentum_3d"] > 0:
                    mr *= 1.2
                score += 0.20 * max(0, mr)

        # 4. Streak bonus (15%)
        if pd.notna(r.get("positive_streak")) and r["positive_streak"] >= 6:
            streak_score = min(r["positive_streak"] / 18, 1.0)
            score += 0.15 * streak_score

        return score if score > 0.20 else 0.0

    def _composite_exit(self, pos, r) -> bool:
        """Composite exit logic."""
        exit_score = 0.0

        if pd.notna(r.get("fr_momentum_7d")) and r["fr_momentum_7d"] < -0.00005:
            exit_score += 0.3
        if pd.notna(r.get("ts_slope")) and r["ts_slope"] < -0.00003:
            exit_score += 0.25
        if pd.notna(r.get("zscore_7d")) and r["zscore_7d"] < -0.5:
            exit_score += 0.2
        if r["funding_rate"] < -0.0001:
            exit_score += 0.35
        if pos.periods_held > 90 and pos.return_on_collateral < 0.003:
            exit_score += 0.25

        return exit_score >= 0.45

    # ========================================================================
    # COMMON STEP FUNCTION
    # ========================================================================
    def _collect_period_pnl(self, snap: pd.DataFrame, pos: CarryPosition, row: pd.Series) -> float:
        """Collect funding and basis MtM for a single position."""
        rate = row["funding_rate"]
        if pos.direction == "long":
            payment = pos.collateral * rate
        else:
            payment = pos.collateral * (-rate)
        pos.funding_collected += payment

        # Basis mark-to-market
        spot_col = "spot_close"
        perp_col = "perp_close"
        if spot_col in row.index and perp_col in row.index:
            spot_price = row[spot_col]
            perp_price = row[perp_col]
            if pd.notna(spot_price) and pd.notna(perp_price) and spot_price > 0 and perp_price > 0:
                pos.update_basis_mtm(float(spot_price), float(perp_price))

        pos.periods_held += 1
        pos.peak_pnl = max(pos.peak_pnl, pos.net_pnl)
        return payment

    def _step_carry(self, ts, snap, step_idx, total_steps, entry_fn, exit_fn):
        """Common step for single-exchange carry strategies."""

        # 1. Collect funding + basis MtM
        for key, pos in list(self.state.positions.items()):
            row = snap[snap["symbol"] == pos.symbol]
            if row.empty:
                continue
            self._collect_period_pnl(snap, pos, row.iloc[0])

        # 2. Risk checks
        if self.state.drawdown > self.config.max_drawdown_exit:
            for key in list(self.state.positions.keys()):
                self._close_position(ts, key, "max_drawdown")

        # 3. Per-position exit
        for key, pos in list(self.state.positions.items()):
            row = snap[snap["symbol"] == pos.symbol]
            if row.empty:
                continue
            r = row.iloc[0]

            # Universal loss limit
            if pos.net_pnl < -self.state.equity * self.config.max_single_loss_pct:
                self._close_position(ts, key, "max_loss")
                continue

            # Strategy-specific exit
            if exit_fn(pos, r):
                self._close_position(ts, key, "signal_exit")

        # 4. Entry
        if self._can_open():
            candidates = []
            for _, r in snap.iterrows():
                sym = r["symbol"]
                key = f"{sym}_binance"
                if key in self.state.positions:
                    continue
                score = entry_fn(r)
                if score > 0:
                    candidates.append((sym, r, score))

            candidates.sort(key=lambda x: x[2], reverse=True)
            for sym, r, score in candidates:
                if not self._can_open():
                    break
                spot_price = r.get("spot_close", 0.0)
                perp_price = r.get("perp_close", 0.0)
                self._open_position(
                    ts, sym, "binance", r["funding_rate"],
                    spot_price=spot_price, perp_price=perp_price,
                    direction="long",
                )

        # 5. Record equity
        self.state.equity_history.append({
            "timestamp": ts,
            "equity": self.state.equity,
            "positions": len(self.state.positions),
            "drawdown": self.state.drawdown,
            "locked": self.state.locked_collateral,
        })

        if (step_idx + 1) % 500 == 0:
            print(f"  Period {step_idx+1}/{total_steps}: equity=${self.state.equity:,.0f}, "
                  f"pos={len(self.state.positions)}, dd={self.state.drawdown:.2%}")

    def run(self, df: pd.DataFrame) -> PortfolioState:
        """Run the selected strategy."""
        strategy_map = {
            "adaptive_carry": self.run_adaptive_carry,
            "cross_exchange": self.run_cross_exchange,
            "term_structure": self.run_term_structure,
            "mean_reversion": self.run_mean_reversion,
            "composite": self.run_composite,
        }
        runner = strategy_map.get(self.config.strategy)
        if runner is None:
            raise ValueError(f"Unknown strategy: {self.config.strategy}")
        return runner(df)


# ============================================================================
# METRICS & REPORTING
# ============================================================================

def compute_metrics(state: PortfolioState) -> dict:
    """Compute comprehensive backtest metrics."""
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
    drawdown = (cumulative - peak) / peak
    max_dd = drawdown.min()

    calmar = cagr / abs(max_dd) if max_dd != 0 else 0

    # Trade stats
    trade_df = pd.DataFrame(state.trade_log) if state.trade_log else pd.DataFrame()
    total_trades = len(trade_df)
    win_rate = 0
    avg_win = 0
    avg_loss = 0
    profit_factor = 0

    if not trade_df.empty and "net_pnl" in trade_df.columns:
        closes = trade_df[trade_df["action"] == "CLOSE"]
        if len(closes) > 0:
            winners = closes[closes["net_pnl"] > 0]
            losers = closes[closes["net_pnl"] <= 0]
            win_rate = len(winners) / len(closes)
            avg_win = winners["net_pnl"].mean() if len(winners) > 0 else 0
            avg_loss = losers["net_pnl"].mean() if len(losers) > 0 else 0
            total_wins = winners["net_pnl"].sum() if len(winners) > 0 else 0
            total_losses = abs(losers["net_pnl"].sum()) if len(losers) > 0 else 0
            profit_factor = total_wins / total_losses if total_losses > 0 else float("inf")

    return {
        "sharpe": round(float(sharpe), 2),
        "annual_return_pct": round(float(cagr) * 100, 1),
        "max_drawdown_pct": round(float(max_dd) * 100, 2),
        "sortino": round(float(sortino), 2),
        "calmar": round(float(calmar), 2),
        "total_return_pct": round(float(total_return) * 100, 1),
        "total_trades": total_trades,
        "win_rate_pct": round(float(win_rate) * 100, 1),
        "avg_win": round(float(avg_win), 2),
        "avg_loss": round(float(avg_loss), 2),
        "profit_factor": round(float(profit_factor), 2) if profit_factor != float("inf") else "inf",
        "backtest_start": str(eq_df.index[0]),
        "backtest_end": str(eq_df.index[-1]),
        "n_periods": len(eq_df),
        "n_years": round(n_years, 2),
        "final_equity": round(float(equity.iloc[-1]), 2),
        "initial_capital": round(float(state.initial_capital), 2),
    }


def save_results(state: PortfolioState, strategy_name: str, output_dir: str = "results"):
    """Save backtest results."""
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    metrics = compute_metrics(state)

    with open(out / f"metrics_{strategy_name}.json", "w") as f:
        json.dump(metrics, f, indent=2, default=str)

    eq_df = pd.DataFrame(state.equity_history)
    eq_df.to_csv(out / f"equity_{strategy_name}.csv", index=False)

    if state.trade_log:
        trade_df = pd.DataFrame(state.trade_log)
        trade_df.to_csv(out / f"trades_{strategy_name}.csv", index=False)

    return metrics
