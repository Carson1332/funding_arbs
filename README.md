# Funding Rate Arbitrage

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A delta-neutral cryptocurrency funding rate arbitrage research framework with rigorous backtest methodology. The system collects historical 8-hour funding rate data across perpetual futures on **Binance**, **Bybit**, and **OKX**, then backtests strategies that harvest funding payments in both positive and negative funding regimes.

> **Note:** This repository has undergone a rigorous methodological review. Previous versions reported Sharpe ratios > 6.0 due to data alignment bugs and incorrect fee arithmetic. The current version implements exact timestamp alignment, correct 50 bps round-trip fee accounting, and explicit opportunity cost deduction. The realistic Sharpe ratio for the best configuration is **1.97**. See `ANALYSIS.md` for the full diagnostic report.

---

## What This Project Does

This framework implements and compares multiple approaches to funding rate arbitrage:

1. **Data Ingestion** — Downloads and caches 8-hour funding rates, OHLCV prices, and open-interest data via `ccxt` and optional CoinAPI.
2. **Signal Generation** — Computes composite signals including funding z-scores, basis momentum, open-interest concentration, and term structure.
3. **Backtest Engine** — Replays historical funding snapshots with realistic fee models (spot + perp + slippage), opportunity cost deduction, and path-dependent risk management (equity-scaled sizing and drawdown limits).
4. **Strategy Evolution** — Documents the full journey from a failing mean-reversion baseline, to a working adaptive carry strategy, to a bidirectional engine that captures yield in both positive and negative funding regimes.

### Core Idea

Perpetual futures exchanges use funding rates to anchor perp prices to spot. In crypto, the structural long bias creates persistently positive funding rates. By going **long spot + short perpetual**, a trader collects these funding payments with near-zero directional exposure. When funding turns negative, the position flips — **short spot + long perpetual** — harvesting payments from the other side.

---

## The Reality Check: Bugs, Diagnoses, and Fixes

During the methodological review, we discovered several critical flaws in the original backtest that artificially inflated the Sharpe ratio from ~1.97 to > 6.0. Here is the full accounting of every problem found, how it was diagnosed, and how it was solved.

### 1. The Data Alignment Bug (Phantom Timestamps)

**The Problem:** The backtest engine iterates over a chronological grid of timestamps. We discovered that the grid had 3,357 unique timestamps instead of the expected 2,522 (for a 2.3-year period at 8-hour intervals). Furthermore, the intersection of BTC and TIA data was only 66.9%, despite both covering the exact same date range.

**The Diagnosis:** 
1. **Sub-second API Jitter:** Binance API returns timestamps with fractional milliseconds (e.g., `2024-01-05 00:00:00.001000`). When merged with clean timestamps, pandas treated these as distinct rows, creating 835 "phantom" duplicate time slots where some symbols were invisible to others.
2. **Resample Misalignment:** Symbols with native 4-hour intervals (like TIA and WIF) were aggregated to 8-hour buckets using `pd.resample("8h", origin="start_day")`. Without explicit boundary labels, pandas defaulted to `closed='right', label='right'`, which shifted the 4-hour data by 8 hours, breaking alignment with native 8-hour symbols.

**The Fix:** 
- Added `dt.floor("s")` to all timestamps immediately upon loading to strip sub-second jitter.
- Added `closed="left", label="left"` to the resample function to ensure `[00:00, 08:00)` correctly maps to `00:00`.
- *Result:* The timestamp grid compacted to exactly 2,522 periods, and the BTC/TIA intersection returned to 100.0%.

### 2. The Fee Arithmetic Bug (Understated Costs)

**The Problem:** The original report claimed a "Realistic" fee scenario of 5 bps per round trip, which is impossible for retail or low-tier VIP traders crossing the spread on two legs.

**The Diagnosis:** The fee formula in the report and the engine's cost attribution were misaligned. Slippage must be applied to *both* the spot and perpetual legs.

**The Fix:** 
- Explicitly defined the Realistic scenario: 10 bps spot taker + 5 bps perp taker + 5 bps slippage per leg.
- Correct formula: `(10 + 5 + 5×2) = 25 bps per entry` = **50 bps per round trip**.
- *Result:* Trading costs consumed 10.87% of the gross funding income, reducing the net PnL drastically.

### 3. The Opportunity Cost Omission

**The Problem:** The strategy locks up capital in spot assets and perpetual margin. The original backtest treated the gross funding income as pure profit, ignoring the risk-free rate that could be earned by simply lending stablecoins.

**The Fix:** 
- Implemented a 4% annual opportunity cost.
- Crucially, this cost is charged *only on the actually deployed collateral* for the exact duration of each trade, and deducted directly from the gross funding income.
- *Result:* Opportunity cost consumed another 5.96% of the gross funding income.

### 4. The Path-Dependence Illusion

**The Problem:** When running the parameter sweep across Optimistic, Realistic, and Pessimistic fee scenarios, we noticed the Pessimistic scenario executed 379 round trips compared to 238 for the Realistic scenario. A fee filter should *reduce* trades, not increase them.

**The Diagnosis:** The backtest engine does not filter trades by `expected carry > cost`. Instead, the higher fees in the Pessimistic scenario eroded equity faster. This triggered the portfolio-level `max_drawdown` limit (8%) 110 distinct times, forcing the liquidation of 161 positions. Of those, 134 were subsequently re-opened when the signal persisted, creating a churn spiral.

**The Fix:** 
- Acknowledged that the three fee scenarios are **different path-dependent simulations**, not the same strategy with costs subtracted post-hoc.
- Added explicit warnings about the strategy's fragility in high-fee environments due to risk-management feedback loops.

### 5. The Signal Quality Revelation (Fat-Tail Carry)

**The Problem:** Despite the strategy being profitable overall, a deep dive into the trade logs revealed that 25 out of 43 symbols were net-negative contributors after costs.

**The Diagnosis:** We computed the per-trade carry as a fraction of position notional. The median per-trade carry was only 19.7 bps, while the round-trip break-even cost was 50 bps. 
- 64.7% of historical trades failed to clear break-even.
- 89.9% of trades failed to clear a 3× break-even hurdle (150 bps).
- However, the 10.1% of trades that exceeded 150 bps carry generated +4.79% net PnL, driving all the strategy's alpha.

**The Fix:** 
- Reclassified the strategy from a "consistent yield generator" to a **"fat-tail carry play."** The entry signal systematically misjudges expected carry for most symbols, relying on a few massive funding spikes (like WIF and AXS) to cover the bleed from the rest.

---

## Results

### Parameter Sweep: Full Comparison

All figures use realistic VIP-0 tier fees (50 bps round trip) and deduct a 4% annual opportunity cost.

| Strategy | Sharpe | Ann. Return | Max DD | Win Rate | Trades |
|----------|--------|-------------|--------|----------|--------|
| **Carry Strict** | **1.97** | **1.42%** | -1.80% | 46.0% | 253 |
| Carry Concentrated | 1.88 | 1.61% | -1.70% | 46.1% | 179 |
| Carry Diversified | 0.66 | 0.44% | -3.00% | 35.3% | 478 |
| Carry Moderate | 0.44 | 0.33% | -3.65% | 35.3% | 342 |

---

## Known Limitations

1. **Basis Mark-to-Market Not Modeled**: The current backtest does not merge spot/perp prices, meaning basis MtM is structurally zero. In reality, basis convergence/divergence creates unrealized PnL that affects margin requirements and risk management.
2. **Execution Slippage**: The backtest assumes execution at the closing mark price. In reality, crossing the spread on both spot and perp legs will incur additional slippage.
3. **Exchange Risk**: The strategy requires holding capital on centralized exchanges. The yield premium is partially a risk premium for exchange counterparty risk.
4. **In-Sample Bias**: The current parameter sweep is entirely in-sample. A Walk-Forward Out-Of-Sample (OOS) test splitting 2024 (train) vs 2025-2026 (test) is required before live deployment.
5. **Survivorship Bias**: Only currently-listed symbols are included. Delisted symbols are excluded, biasing results upward. Given the strategy's ultra-low volatility (0.72%), the Sharpe metric is statistically fragile — every 1% of return inflation artificially boosts Sharpe by ~1.4 units.

---

## How to Use

### Workflow After Cloning

```bash
# 1. Clone and enter the repository
git clone https://github.com/Carson1332/funding_arbs.git
cd funding_arbs

# 2. Install dependencies
pip install -e ".[dev]"

# 3. Download historical data (cached locally, not committed)
python -m data.downloader --config config/default.yaml

# 4. Run backtests
python run_parameter_sweep.py              # unidirectional carry sweep
python run_bidirectional_sweep.py          # bidirectional carry sweep

# 5. Generate diagnostic reports
python diagnostics.py                      # exact attribution and signal quality
python validate_alignment.py               # verify timestamp alignment
```

> **Note:** The repository contains backtest logic, configuration, and summary images. Raw equity curves and trade logs are generated locally and excluded from git by `.gitignore`. After cloning, follow the steps above to reproduce all results.

---

## Project Structure

```
funding_arbs/
├── config/
│   ├── default.yaml          # Global parameters
│   ├── fees.yaml             # Fee scenario definitions (Opt/Real/Pess)
│   └── universe.yaml         # Tracked perpetual pairs
├── data/
│   ├── downloader.py         # Funding rate ingestion (ccxt + CoinAPI)
│   ├── spot_prices.py        # OHLCV for basis calculation
│   └── oi_fetcher.py         # Open interest downloader
├── research/
│   ├── funding_zscore.py     # Rolling z-score signals
│   ├── basis_momentum.py     # Perp-spot basis momentum
│   └── kalman_hedge.py       # Dynamic hedge ratio (Kalman filter)
├── backtest/
│   ├── enhanced_engine.py    # Carry-trade backtest engine
│   └── fee_model.py          # Realistic cost model
├── results/
│   └── sweep/                # Backtest outputs (metrics, equity, tearsheets)
├── diagnostics.py            # Exact attribution and reconciliation script
├── validate_alignment.py     # Timestamp alignment verification
├── ANALYSIS.md               # Diagnostic report
└── README.md                 # This file
```

---

## License

This project is licensed under the [MIT License](LICENSE).

```
MIT License

Copyright (c) 2026 Carson1332
```
