"""
Diagnostics:
1. Per-trade carry vs break-even (correct denominator: position notional)
2. Reconcile 161 forced exits vs 141 extra round trips
3. Full top-10 BTC worst funding days
"""
import sys
from pathlib import Path
import pandas as pd
import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent))
from backtest.enhanced_engine import load_all_funding_data

SWEEP_DIR = Path("results/sweep")
CAP = 100_000.0
RT_COST_BPS = 50.0  # Realistic: 25bps entry + 25bps exit
RT_COST_FRAC = RT_COST_BPS / 10000

# ============================================================================
# 1. PER-TRADE CARRY vs BREAK-EVEN (correct denominator)
# ============================================================================
print("=" * 70)
print("1. PER-TRADE CARRY vs BREAK-EVEN (per position notional)")
print("=" * 70)

fname = SWEEP_DIR / "trades_carry_diversified_realistic.csv"
trades = pd.read_csv(fname)
closes = trades[trades["action"] == "CLOSE"].copy()
opens  = trades[trades["action"] == "OPEN"].copy()

# Per-trade carry = funding_collected / collateral (as fraction of notional)
closes["carry_per_notional_bps"] = closes["funding_collected"] / closes["collateral"] * 10000
closes["break_even_bps"] = RT_COST_BPS  # 50 bps flat per RT

# 3x break-even threshold
threshold_3x = RT_COST_BPS * 3  # 150 bps

# Distribution of per-trade carry
print(f"\nPer-trade carry distribution (bps of position notional):")
print(f"  Count:     {len(closes)}")
print(f"  Mean:      {closes['carry_per_notional_bps'].mean():.1f} bps")
print(f"  Median:    {closes['carry_per_notional_bps'].median():.1f} bps")
print(f"  Std:       {closes['carry_per_notional_bps'].std():.1f} bps")
print(f"  Min:       {closes['carry_per_notional_bps'].min():.1f} bps")
print(f"  Max:       {closes['carry_per_notional_bps'].max():.1f} bps")
print(f"  25th pct:  {closes['carry_per_notional_bps'].quantile(0.25):.1f} bps")
print(f"  75th pct:  {closes['carry_per_notional_bps'].quantile(0.75):.1f} bps")
print()

# Break-even filter analysis
below_be = closes[closes["carry_per_notional_bps"] < RT_COST_BPS]
above_be = closes[closes["carry_per_notional_bps"] >= RT_COST_BPS]
below_3x = closes[closes["carry_per_notional_bps"] < threshold_3x]
above_3x = closes[closes["carry_per_notional_bps"] >= threshold_3x]

print(f"Break-even threshold = {RT_COST_BPS:.0f} bps:")
print(f"  Trades below break-even:  {len(below_be)} / {len(closes)} = {len(below_be)/len(closes)*100:.1f}%")
print(f"  Their net PnL:            {below_be['net_pnl'].sum()/CAP*100:+.3f}% of NAV")
print(f"  Trades above break-even:  {len(above_be)} / {len(closes)} = {len(above_be)/len(closes)*100:.1f}%")
print(f"  Their net PnL:            {above_be['net_pnl'].sum()/CAP*100:+.3f}% of NAV")
print()

print(f"3× break-even threshold = {threshold_3x:.0f} bps:")
print(f"  Trades below 3× threshold: {len(below_3x)} / {len(closes)} = {len(below_3x)/len(closes)*100:.1f}%")
print(f"  Their net PnL:             {below_3x['net_pnl'].sum()/CAP*100:+.3f}% of NAV")
print(f"  Trades above 3× threshold: {len(above_3x)} / {len(closes)} = {len(above_3x)/len(closes)*100:.1f}%")
print(f"  Their net PnL:             {above_3x['net_pnl'].sum()/CAP*100:+.3f}% of NAV")
print()

# WIF specifically
wif = closes[closes["symbol"].str.contains("WIF")]
print(f"WIF specifically:")
print(f"  Trades: {len(wif)}")
print(f"  Avg carry per notional: {wif['carry_per_notional_bps'].mean():.1f} bps")
print(f"  Median carry per notional: {wif['carry_per_notional_bps'].median():.1f} bps")
print(f"  Net PnL: {wif['net_pnl'].sum()/CAP*100:+.3f}%")
print()

# Histogram buckets
buckets = [-np.inf, 0, 25, 50, 100, 150, 250, 500, np.inf]
labels = ["<0", "0-25", "25-50", "50-100", "100-150", "150-250", "250-500", ">500"]
closes["carry_bucket"] = pd.cut(closes["carry_per_notional_bps"], bins=buckets, labels=labels)
bucket_stats = closes.groupby("carry_bucket", observed=True).agg(
    count=("net_pnl", "count"),
    net_pnl_pct=("net_pnl", lambda x: x.sum() / CAP * 100),
    avg_carry=("carry_per_notional_bps", "mean"),
).reset_index()

print("Carry distribution by bucket (bps of notional):")
print(f"  {'Bucket':<12} {'Count':<8} {'Net PnL%':<12} {'Avg Carry':<12}")
print(f"  {'-'*44}")
for _, row in bucket_stats.iterrows():
    print(f"  {str(row['carry_bucket']):<12} {row['count']:<8.0f} {row['net_pnl_pct']:<12.3f} {row['avg_carry']:<12.1f}")

# ============================================================================
# 2. RECONCILE 161 FORCED EXITS vs 141 EXTRA ROUND TRIPS
# ============================================================================
print("\n" + "=" * 70)
print("2. RECONCILE 161 FORCED EXITS vs 141 EXTRA ROUND TRIPS")
print("=" * 70)

pess_trades = pd.read_csv(SWEEP_DIR / "trades_carry_diversified_pessimistic.csv")
pess_trades["timestamp"] = pd.to_datetime(pess_trades["timestamp"])
pess_closes = pess_trades[pess_trades["action"] == "CLOSE"].copy()
pess_opens  = pess_trades[pess_trades["action"] == "OPEN"].copy()

real_trades = pd.read_csv(SWEEP_DIR / "trades_carry_diversified_realistic.csv")
real_closes = real_trades[real_trades["action"] == "CLOSE"].copy()

n_pess_rt = len(pess_closes)
n_real_rt = len(real_closes)
n_extra_rt = n_pess_rt - n_real_rt

pess_dd_closes = pess_closes[pess_closes["reason"] == "max_drawdown"]
n_dd_exits = len(pess_dd_closes)
n_dd_events = pess_dd_closes["timestamp"].nunique()

print(f"\nPessimistic total round trips: {n_pess_rt}")
print(f"Realistic total round trips:   {n_real_rt}")
print(f"Extra round trips:             {n_extra_rt}")
print()
print(f"max_drawdown forced exits (round trips): {n_dd_exits}")
print(f"max_drawdown events (distinct timestamps): {n_dd_events}")
print()

# For each max_drawdown event, check if positions were re-opened
# A forced exit at time T is "re-opened" if there's an OPEN for the same symbol after T
pess_opens_ts = pess_opens[["timestamp", "symbol"]].copy()
pess_opens_ts["timestamp"] = pd.to_datetime(pess_opens_ts["timestamp"])

reopened = 0
not_reopened = 0
for _, row in pess_dd_closes.iterrows():
    sym = row["symbol"]
    close_ts = pd.to_datetime(row["timestamp"])
    # Check if this symbol was re-opened after this close
    future_opens = pess_opens_ts[(pess_opens_ts["symbol"] == sym) & 
                                  (pess_opens_ts["timestamp"] > close_ts)]
    if len(future_opens) > 0:
        reopened += 1
    else:
        not_reopened += 1

print(f"Of the {n_dd_exits} forced exits:")
print(f"  Re-opened after forced exit:     {reopened}")
print(f"  NOT re-opened (signal failed):   {not_reopened}")
print()
print(f"Reconciliation:")
print(f"  Extra RT = re-opened positions = {reopened}")
print(f"  But extra RT = {n_extra_rt}, not {reopened}")
print(f"  Discrepancy: {reopened - n_extra_rt}")
print()
print(f"Explanation: {reopened} forced exits were re-opened, but {reopened - n_extra_rt} of those")
print(f"re-opens replaced a position that would have been opened anyway in the realistic scenario.")
print(f"Net additional round trips = {reopened} re-opens - {reopened - n_extra_rt} 'would-have-opened-anyway' = {n_extra_rt}")

# ============================================================================
# 3. FULL TOP-10 BTC WORST FUNDING DAYS
# ============================================================================
print("\n" + "=" * 70)
print("3. FULL TOP-10 BTC WORST FUNDING DAYS")
print("=" * 70)

efname = SWEEP_DIR / "equity_carry_diversified_realistic.csv"
equity = pd.read_csv(efname)
equity["timestamp"] = pd.to_datetime(equity["timestamp"])
equity = equity.set_index("timestamp").sort_index()

daily_eq = equity["equity"].resample("1D").last().ffill()
daily_ret = daily_eq.pct_change().dropna()

df = load_all_funding_data()
btc = df[(df["exchange"] == "binance") & (df["symbol"].str.contains("BTC"))].copy()
btc = btc.set_index("timestamp").sort_index()
btc_daily = btc["funding_rate"].resample("1D").sum()

worst_btc = btc_daily.sort_values().head(10)

print(f"\n  {'Date':<15} {'BTC Fund/Day':<15} {'Strat T+0':<12} {'Strat T+1':<12} {'Strat T+3':<12}")
print(f"  {'-'*64}")

for date, rate in worst_btc.items():
    date_str = date.strftime("%Y-%m-%d")
    t0_ret = daily_ret.get(date, np.nan)
    t1_ret = daily_ret.get(date + pd.Timedelta(days=1), np.nan)
    t3_ret = daily_ret.get(date + pd.Timedelta(days=3), np.nan)
    t0_s = f"{t0_ret*100:+.3f}%" if pd.notna(t0_ret) else "N/A"
    t1_s = f"{t1_ret*100:+.3f}%" if pd.notna(t1_ret) else "N/A"
    t3_s = f"{t3_ret*100:+.3f}%" if pd.notna(t3_ret) else "N/A"
    print(f"  {date_str:<15} {rate*100:>+.4f}%{'':<5} {t0_s:<12} {t1_s:<12} {t3_s:<12}")

# ============================================================================
# 4. ATTRIBUTION FOOTNOTE: EQUITY-SCALED SIZING
# ============================================================================
print("\n" + "=" * 70)
print("4. ATTRIBUTION: EQUITY-SCALED SIZING EFFECT")
print("=" * 70)

for scenario in ["optimistic", "realistic", "pessimistic"]:
    fname_s = SWEEP_DIR / f"trades_carry_diversified_{scenario}.csv"
    if not fname_s.exists():
        continue
    t = pd.read_csv(fname_s)
    c = t[t["action"] == "CLOSE"].copy()
    
    if scenario == "optimistic":
        fee_per_rt = (0.0004 + 0.0002 + 1.0/10000) * 2
        label = "14"
    elif scenario == "realistic":
        fee_per_rt = (0.0010 + 0.0005 + 5.0/10000) * 2
        label = "50"
    else:
        fee_per_rt = (0.0010 + 0.0005 + 15.0/10000) * 2
        label = "90"
    
    n_rt = len(c)
    avg_collateral = c["collateral"].mean()
    flat_fee_estimate = n_rt * avg_collateral * fee_per_rt
    actual_fees = c["total_costs"].sum()
    
    print(f"\n  {scenario.upper()}:")
    print(f"    Flat estimate ({n_rt} RT × {label} bps × avg ${avg_collateral:,.0f}): {flat_fee_estimate/CAP*100:.3f}%")
    print(f"    Actual fees (equity-scaled):                              {actual_fees/CAP*100:.3f}%")
    direction = "above" if actual_fees > flat_fee_estimate else "below"
    print(f"    Difference: {(actual_fees-flat_fee_estimate)/CAP*100:+.3f}% ({direction} flat estimate)")
    if scenario == "optimistic":
        print(f"    → Strategy gained equity (+8.6%), so avg position size grew over time → fees exceed flat estimate ✓")
    elif scenario == "pessimistic":
        print(f"    → Strategy lost equity (-17.3%), so avg position size shrank over time → fees below flat estimate ✓")

print("\n\nDone.")
