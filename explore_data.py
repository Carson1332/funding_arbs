"""Explore the funding rate data to understand characteristics."""
import pandas as pd
import numpy as np
import glob

# Load all parquet files
files = glob.glob('data/cache/funding_rates/*.parquet')
print(f'Total parquet files: {len(files)}')

all_dfs = []
for f in files:
    try:
        d = pd.read_parquet(f)
        all_dfs.append(d)
    except Exception as e:
        print(f"Error loading {f}: {e}")

all_df = pd.concat(all_dfs, ignore_index=True)
print(f'\n=== ALL DATA ===')
print(f'Total records: {len(all_df)}')
print(f'Columns: {list(all_df.columns)}')
print(f'Symbols: {all_df["symbol"].nunique()}')
print(f'Exchanges: {all_df["exchange"].unique()}')
print(f'Date range: {all_df["timestamp"].min()} to {all_df["timestamp"].max()}')

print(f'\nFunding rate stats:')
print(all_df['funding_rate'].describe())

ann_rate = all_df['funding_rate'] * 3 * 365
print(f'\nAnnualised rate stats:')
print(ann_rate.describe())

print(f'\nPositive rate %: {(all_df["funding_rate"] > 0).mean():.2%}')
print(f'Rate > 0.01% (ann ~10.95%): {(all_df["funding_rate"] > 0.0001).mean():.2%}')
print(f'Rate > 0.03% (ann ~32.85%): {(all_df["funding_rate"] > 0.0003).mean():.2%}')
print(f'Rate > 0.05% (ann ~54.75%): {(all_df["funding_rate"] > 0.0005).mean():.2%}')

# Per-symbol stats
print(f'\n=== PER-SYMBOL MEAN ANNUALISED RATE (Binance) ===')
binance = all_df[all_df['exchange'] == 'binance']
sym_stats = binance.groupby('symbol')['funding_rate'].agg(['mean', 'std', 'count'])
sym_stats['ann_mean'] = sym_stats['mean'] * 3 * 365
sym_stats['ann_std'] = sym_stats['std'] * 3 * 365
sym_stats = sym_stats.sort_values('ann_mean', ascending=False)
print(sym_stats.head(20).to_string())

# Cross-exchange spread
print(f'\n=== CROSS-EXCHANGE FUNDING SPREAD ===')
pivot = all_df.pivot_table(index=['timestamp', 'symbol'], columns='exchange', values='funding_rate')
if 'binance' in pivot.columns and 'bybit' in pivot.columns:
    spread = pivot['binance'] - pivot['bybit']
    print(f'Binance - Bybit spread stats:')
    print(spread.describe())
    print(f'Annualised spread: {spread.mean() * 3 * 365:.4f}')

# Time series characteristics
print(f'\n=== AUTOCORRELATION OF FUNDING RATES ===')
for sym in ['BTC/USDT:USDT', 'ETH/USDT:USDT', 'SOL/USDT:USDT']:
    sub = binance[binance['symbol'] == sym].sort_values('timestamp')['funding_rate']
    if len(sub) > 10:
        ac1 = sub.autocorr(lag=1)
        ac3 = sub.autocorr(lag=3)
        ac9 = sub.autocorr(lag=9)
        print(f'{sym}: AC(1)={ac1:.3f}, AC(3)={ac3:.3f}, AC(9)={ac9:.3f}')

# Regime analysis
print(f'\n=== FUNDING RATE REGIMES ===')
binance_ts = binance.groupby('timestamp')['funding_rate'].mean()
binance_ts = binance_ts.sort_index()
print(f'Mean rate over time (monthly):')
monthly = binance_ts.resample('M').mean()
for idx, val in monthly.items():
    print(f'  {idx.strftime("%Y-%m")}: {val:.6f} (ann: {val*3*365:.2%})')
