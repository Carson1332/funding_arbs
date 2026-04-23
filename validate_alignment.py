"""
Validate the alignment fix:
1. Timestamp grid should be ~2522 (not 3357)
2. BTC ∩ TIA should be ~2522 (not 1687)
3. Funding income conservation: raw sum ≈ aggregated sum per symbol
4. All timestamps on canonical hours [0, 8, 16]
5. No dt_hours column leaking into output
"""
import sys
from pathlib import Path
import pandas as pd
import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent))
from backtest.enhanced_engine import load_all_funding_data

print("=" * 70)
print("VALIDATION: ALIGNMENT FIX")
print("=" * 70)

df = load_all_funding_data(merge_prices=False)
print(f"Total records: {len(df):,}")
print(f"Columns: {list(df.columns)}")

# Check 1: No dt_hours column
assert "dt_hours" not in df.columns, "dt_hours should be dropped!"
print("✓ dt_hours column dropped")

# Check 2: All canonical hours
non_canonical = df[~df["timestamp"].dt.hour.isin([0, 8, 16])]
assert len(non_canonical) == 0, f"Found {len(non_canonical)} non-canonical timestamps!"
print("✓ All timestamps on canonical hours [0, 8, 16]")

# Check 3: Binance timestamp grid
binance = df[df["exchange"] == "binance"]
unique_ts = binance["timestamp"].nunique()
print(f"\nBinance unique timestamps: {unique_ts}")
assert unique_ts < 2600, f"Expected ~2522, got {unique_ts} — still fragmented!"
print("✓ Timestamp grid is compact (no fragmentation)")

# Check 4: BTC ∩ TIA intersection
btc = binance[binance["symbol"].str.contains("BTC")]
tia = binance[binance["symbol"].str.contains("TIA")]
wif = binance[binance["symbol"].str.contains("WIF")]

btc_ts = set(btc["timestamp"])
tia_ts = set(tia["timestamp"])
wif_ts = set(wif["timestamp"])

btc_tia_intersection = len(btc_ts & tia_ts)
btc_wif_intersection = len(btc_ts & wif_ts)

print(f"\nBTC records: {len(btc)}, TIA records: {len(tia)}, WIF records: {len(wif)}")
print(f"BTC ∩ TIA: {btc_tia_intersection} / {len(btc_ts)} ({btc_tia_intersection/len(btc_ts)*100:.1f}%)")
print(f"BTC ∩ WIF: {btc_wif_intersection} / {len(btc_ts)} ({btc_wif_intersection/len(btc_ts)*100:.1f}%)")

assert btc_tia_intersection == len(btc_ts), f"BTC ∩ TIA should be {len(btc_ts)}, got {btc_tia_intersection}"
print("✓ BTC and TIA fully aligned")

# Check 5: Funding income conservation
cache_dir = Path(__file__).resolve().parent / "data" / "cache" / "funding_rates"

# TIA
tia_raw = pd.read_parquet(list(cache_dir.glob("binance_TIA*"))[0])
tia_raw_sum = tia_raw["funding_rate"].sum()
tia_agg_sum = tia["funding_rate"].sum()
print(f"\nTIA funding conservation:")
print(f"  Raw sum:  {tia_raw_sum:.10f}")
print(f"  Agg sum:  {tia_agg_sum:.10f}")
print(f"  Diff:     {abs(tia_raw_sum - tia_agg_sum):.2e}")
assert abs(tia_raw_sum - tia_agg_sum) < 1e-8, "TIA funding not conserved!"
print("✓ TIA funding income conserved")

# WIF
wif_raw = pd.read_parquet(list(cache_dir.glob("binance_WIF*"))[0])
wif_raw_sum = wif_raw["funding_rate"].sum()
wif_agg_sum = wif["funding_rate"].sum()
print(f"\nWIF funding conservation:")
print(f"  Raw sum:  {wif_raw_sum:.10f}")
print(f"  Agg sum:  {wif_agg_sum:.10f}")
print(f"  Diff:     {abs(wif_raw_sum - wif_agg_sum):.2e}")
assert abs(wif_raw_sum - wif_agg_sum) < 1e-8, "WIF funding not conserved!"
print("✓ WIF funding income conserved")

# BTC (native 8h, should be unchanged)
btc_raw = pd.read_parquet(list(cache_dir.glob("binance_BTC*"))[0])
btc_raw_sum = btc_raw["funding_rate"].sum()
btc_agg_sum = btc["funding_rate"].sum()
print(f"\nBTC funding conservation:")
print(f"  Raw sum:  {btc_raw_sum:.10f}")
print(f"  Agg sum:  {btc_agg_sum:.10f}")
print(f"  Diff:     {abs(btc_raw_sum - btc_agg_sum):.2e}")
assert abs(btc_raw_sum - btc_agg_sum) < 1e-8, "BTC funding not conserved!"
print("✓ BTC funding income conserved")

# Check 6: Per-symbol record counts
sym_counts = binance.groupby("symbol").size()
print(f"\nPer-symbol record counts (Binance):")
print(f"  Min: {sym_counts.min()}, Max: {sym_counts.max()}, Mean: {sym_counts.mean():.0f}")
print(f"  Symbols with max count: {sym_counts[sym_counts == sym_counts.max()].index.tolist()[:5]}")

# Check 7: Symbols per timestamp
ts_sym = binance.groupby("timestamp")["symbol"].nunique()
print(f"\nSymbols per timestamp:")
print(f"  Min: {ts_sym.min()}, Max: {ts_sym.max()}, Mean: {ts_sym.mean():.1f}, Median: {ts_sym.median()}")
sparse = ts_sym[ts_sym < 10]
print(f"  Timestamps with < 10 symbols: {len(sparse)}")

# Check 8: Cross-exchange alignment
for exc in sorted(df["exchange"].unique()):
    exc_df = df[df["exchange"] == exc]
    hours = exc_df["timestamp"].dt.hour.value_counts().sort_index()
    print(f"\n{exc}: {len(exc_df):,} records, {exc_df['symbol'].nunique()} symbols")
    print(f"  Hours: {hours.to_dict()}")
    print(f"  Range: {exc_df['timestamp'].min()} to {exc_df['timestamp'].max()}")

print(f"\n{'='*70}")
print("ALL VALIDATION CHECKS PASSED ✓")
print(f"{'='*70}")
