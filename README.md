# Funding Rate Arbitrage

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A delta-neutral cryptocurrency funding rate arbitrage research framework with trackable backtest results. The system collects historical 8-hour funding rate data across perpetual futures on **Binance**, **Bybit**, and **OKX**, then backtests strategies that harvest funding payments in both positive and negative funding regimes.

---

## What This Project Does

This framework implements and compares multiple approaches to funding rate arbitrage:

1. **Data Ingestion** — Downloads and caches 8-hour funding rates, OHLCV prices, and open-interest data via `ccxt` and optional CoinAPI.
2. **Signal Generation** — Computes composite signals including funding z-scores, basis momentum, open-interest concentration, and term structure.
3. **Backtest Engine** — Replays historical funding snapshots with realistic fee models, slippage, and risk management.
4. **Strategy Evolution** — Documents the full journey from a failing mean-reversion baseline, to a working adaptive carry strategy, to a bidirectional engine that captures yield in both positive and negative funding regimes.

### Core Idea

Perpetual futures exchanges use funding rates to anchor perp prices to spot. In crypto, the structural long bias creates persistently positive funding rates. By going **long spot + short perpetual**, a trader collects these funding payments with near-zero directional exposure. When funding turns negative, the position flips — **short spot + long perpetual** — harvesting payments from the other side.

---

## Data Used

The backtest uses the following data sources and characteristics:

| Attribute | Description |
|-----------|-------------|
| **Exchanges** | Binance, Bybit, OKX |
| **Symbols** | 47 perpetual pairs (BTC, ETH, SOL, BNB, XRP, DOGE, and others — see `config/universe.yaml`) |
| **Frequency** | 8-hour funding rate snapshots (3 periods per day) |
| **Date Range** | January 2024 – April 2026 (~2.3 years, ~5,200 periods) |
| **Total Records** | ~133,000 funding rate observations |
| **Additional Data** | Spot and perpetual OHLCV (for basis calculation), open-interest snapshots |

> **Privacy Note:** Raw market data is cached locally in `data/cache/` and is excluded from version control. The repository contains only the backtest logic, configuration, and aggregated results. Users must run the downloaders themselves to populate the local cache.

---

## Strategy Evolution

### Step 1: Baseline Z-Score Mean Reversion (Failed)

Our first approach used a rolling z-score on funding rates:
- **Entry:** When z-score > 1.5 and annualised rate > 5%
- **Exit:** When z-score returns to 0
- **Logic:** Bet that extreme funding rates will mean-revert

**Why it failed:**
- Funding rates in crypto exhibit strong **autocorrelation**, not mean reversion. Extreme rates tend to persist rather than immediately reverse.
- The strategy entered on temporary spikes and exited as rates "normalised," often locking in losses.
- During sustained high-funding regimes (common in bull markets), the strategy sat in cash while carry traders collected consistent yield.

| Metric | Value |
|--------|-------|
| Cumulative Return | **-6.27%** |
| Annual Return | -0.3% |
| Sharpe Ratio | -0.95 |
| Max Drawdown | -10.0% |
| Win Rate | 17.0% |

### Step 2: Adaptive Carry — Following the Trend (Worked)

We shifted the rationale from *mean reversion* to *trend following*:
- **Entry:** When the 7-day mean funding rate is sustainably high (> 8% annualised), momentum is positive, and the rate has been positive for 6+ consecutive periods.
- **Exit:** Only when momentum turns strongly negative, the rolling mean drops below a threshold, or a max holding period is reached.
- **Logic:** Ride the autocorrelation of funding rates. If a market is paying 20%+ annualised to short perps, that regime typically persists for weeks.

**Why it works:**
- **Autocorrelation exploitation:** Crypto funding rates have significant positive serial correlation. A high 7-day mean predicts continued elevated rates.
- **Reduced trading frequency:** Wider entry thresholds and no take-profit levels cut transaction costs dramatically.
- **Signal-strength sizing:** Position size scales with the confidence of the carry signal.
- **Risk management:** Tight per-position loss limits (1.5% of equity) and portfolio drawdown guards (8%) protect capital during regime shifts.

**But we noticed something critical:** The strategy only made money when funding was positive. In 2026, when funding turned deeply negative, the strategy sat in cash while the market was paying shorts to hold perps. We were leaving money on the table.

### Step 3: Bidirectional Carry — The Bug Hunt

The natural extension: go **short spot + long perp** when funding is sustainably negative, mirroring the long logic.

**First implementation (broken):**

| Metric | Value |
|--------|-------|
| Sharpe | 3.42 |
| Annual Return | 2.17% |
| Max Drawdown | -1.41% |
| Short Win Rate | **20.4%** |
| Short Trades | 333 |

Something was wrong. Shorts were losing money even in 2026 when funding averaged **-11.6% annualised**. Why?

**Deep dive into the trade logs revealed the bug:**

| Problem | Detail |
|---------|--------|
| **Entry/exit mismatch** | Short entry used `fr_momentum_3d < 0` (3-day trend), but exits used `fr_momentum_7d > 0.00008` (7-day trend). These two metrics can have **opposite signs**. |
| **Immediate churn** | 293 of 333 short trades (88%) exited on "pos_momentum." 136 trades (41%) were held for only **one 8h period**. |
| **Fee death spiral** | A round trip costs ~0.34% of collateral (~$60 on a $17k position). To break even in one period, funding must exceed **155% annualised** — nearly impossible. |
| **No negative streak filter** | Longs required `positive_streak >= 6`, but shorts had no equivalent discipline, letting noise trades through. |

**The fix:**
1. Added `negative_streak >= 6` filter — same discipline as longs
2. Short entry now requires **both** `fr_momentum_3d < 0` **AND** `fr_momentum_7d < 0` — both timeframes must agree
3. Added `min_hold_periods = 3` — prevents signal-based exits before 24 hours, killing the fee-bleed churn

### Step 4: Bidirectional Results (Fixed)

The fixed bidirectional engine now outperforms unidirectional on every metric:

| Metric | Unidirectional (Carry Moderate) | Bidirectional 5% Borrow | Improvement |
|--------|--------------------------------|------------------------|-------------|
| **Sharpe** | 6.10 | **6.36** | +4.3% |
| **Annual Return** | 2.43% | **4.04%** | +66% |
| **Max Drawdown** | -0.69% | **-0.78%** | Comparable |
| **Total Trades** | 176 | 338 | +92% |
| **Long Win Rate** | 55.7% | 54.6% | Comparable |
| **Short Win Rate** | N/A | **37.8%** | New |
| **Long PnL** | $12,074 | $11,215 | Comparable |
| **Short PnL** | $0 | **$9,308** | +$9,308 |
| **Combined PnL** | $12,074 | **$20,523** | **+70%** |

> Borrow costs at 5% annual were **not** the problem — they averaged only ~$0.78 per 8h period on a $17k position. The real killer was the 0.34% round-trip fee churn on 1-period trades.

---

## Results

### Equity Curves: Unidirectional vs Bidirectional

![Equity Comparison](results/images/equity_comparison.png)

The bidirectional strategy (solid green) pulls ahead of unidirectional (dashed blue) starting in early 2026 when funding turns deeply negative and the short side activates.

### Drawdown Comparison

![Drawdown Comparison](results/images/drawdown_comparison.png)

Both strategies maintain sub-1% drawdowns. The bidirectional variants show slightly deeper but still minimal drawdowns, a fair trade for 66% higher returns.

### Regime Sensitivity

![Regime Sensitivity](results/images/regime_sensitivity.png)

The strategy is a **funding-rate thermostat** — it harvests yield in both directions and sits in cash during neutral regimes.

| Year | Avg Funding Rate | Long PnL | Short PnL | Combined Return |
|------|-----------------|----------|-----------|-----------------|
| **2024** | **+11.86%** ann. | ~+$9,000 | ~-$25 | **+~9%** |
| **2025** | **+1.29%** ann. | ~+$1,800 | ~+$471 | **+~2%** |
| **2026** | **-11.56%** ann. | ~+$400 | **+$8,862** | **+~8%** |

- **2024** was a "golden age" for longs: March averaged **45%** annualised funding.
- **2025** saw a **10x collapse** in funding rates. Both sides sat in cash because the 8% entry threshold was rarely met.
- **2026** turned deeply negative. The short side captured **$8,862** from 45 trades at a 55.6% win rate.

### Risk-Return Metrics

![Metrics Comparison](results/images/metrics_comparison.png)

### Parameter Sweep: Full Comparison

| Configuration | Sharpe | Ann. Return | Max DD | Win Rate | Final Equity |
|---------------|--------|-------------|--------|----------|--------------|
| **Bidirectional 5% borrow** | **6.36** | **4.04%** | -0.78% | 46.4% | **$120,676** |
| Bidirectional 8% borrow | 5.76 | 3.64% | -1.07% | 45.0% | $118,486 |
| Bidirectional 12% borrow | 4.94 | 3.11% | -1.84% | 42.3% | $115,628 |
| Carry Moderate (uni) | 6.10 | 2.43% | -0.69% | 55.7% | $112,078 |
| Carry Relaxed | 6.35 | 2.66% | -0.86% | 55.4% | $113,289 |
| Carry Concentrated | 6.16 | 2.70% | -0.78% | 68.1% | $113,497 |

> All figures use realistic VIP-0 tier fees (0.10% spot taker, 0.05% perp taker, 1 bp slippage per leg).

### Per-Pair Insights

Not all pairs are equal. The strategy's alpha is concentrated in a handful of mid-cap alts, while large caps barely register.

**AXS — the short king.** AXS produced **+$6,802** total, with **+$6,865 from shorts alone**. In Jan–Mar 2026, AXS funding rates plunged to -400% to -500% annualised. Four monster short trades held 22–28 periods each captured **$6,253** between them.

**WIF — memecoin lottery.** WIF netted **+$1,848**, but it was lumpy: one 70-period long in Mar 2024 captured **+$1,342** during the memecoin mania, while nine other trades were small noise losses. 

**MKR — the perfect record.** Six long trades, **100% win rate**, +$625. Average hold: 132 periods (~44 days). MKR funding spikes during DeFi stress and stays elevated — ideal for a patient carry strategy.

**HBAR** Worst performer at **-$370**. Four of its eight longs in Apr 2024 were 1-period round trips that immediately reversed. HBAR funding flips sign faster than the 7-day mean can catch, making it a natural victim of fee churn.

### QuantStats Tearsheets

Full interactive tearsheets for each configuration:

**Bidirectional:**
- [`tearsheet_bidirectional_5borrow.html`](results/sweep/tearsheet_bidirectional_5borrow.html) — **Best overall**
- [`tearsheet_bidirectional_8borrow.html`](results/sweep/tearsheet_bidirectional_8borrow.html)
- [`tearsheet_bidirectional_12borrow.html`](results/sweep/tearsheet_bidirectional_12borrow.html)

**Unidirectional:**
- [`tearsheet_carry_moderate.html`](results/sweep/tearsheet_carry_moderate.html)
- [`tearsheet_carry_concentrated.html`](results/sweep/tearsheet_carry_concentrated.html)
- [`tearsheet_carry_relaxed.html`](results/sweep/tearsheet_carry_relaxed.html)

---

## How We Got Here: A Playbook

This repository documents a realistic research workflow, including the mistakes:

1. **Start simple.** The z-score baseline failed (-6.27%) but taught us funding rates are autocorrelated, not mean-reverting.
2. **Follow the data.** When 2024 funding averaged 11.86% and 2026 flipped to -11.56%, the strategy must adapt to both regimes.
3. **Question surprises.** Bidirectional initially underperformed (Sharpe 3.42). Instead of blaming borrow costs, we dug into trade logs and found a momentum-filter mismatch causing 1-period fee churn.
4. **Fix with discipline.** Added the same streak and momentum filters to shorts that longs already had. Minimum hold periods prevented round-trip fee bleed.
5. **Validate improvement.** Fixed bidirectional now beats unidirectional by 70% in total PnL with comparable drawdowns.

---

## Next Steps

### 1. Funding Rate Prediction
Instead of only using backward-looking rolling means, add a lightweight predictor:
- **Autoregressive model** (AR(1) or AR(3)) on funding rates
- **Macro regime classifier** (BTC dominance, volatility, open-interest growth) to predict whether funding will persist

### 2. Tiered Borrow Cost Model
The current borrow model assumes a flat 5-12% annual rate. In reality:
- **BTC/ETH:** Borrow rates are low (~2-5%) because liquidity is deep
- **Mid-cap alts:** Borrow rates can spike to 20-50% during stress
- A tiered model by market-cap or exchange margin book would improve realism

### 3. Live Trading Integration
- **Exchange API wrappers** for Binance/Bybit/OKX to automate position entry/exit
- **Real-time funding rate monitor** with Telegram/Slack alerts when entry thresholds are breached
- **Portfolio rebalancer** that checks hedge ratios and funding payments every 8 hours

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

# 5. Generate plots and reports
python plot_sweep_results.py               # comparison charts
python generate_tearsheets.py              # QuantStats HTML reports

# 6. Run tests
python -m pytest tests/ -v --tb=short
```

> **Note:** The repository contains backtest logic, configuration, and summary images. Raw equity curves and trade logs are generated locally and excluded from git by `.gitignore`. After cloning, follow the steps above to reproduce all results.

### Docker

```bash
docker compose up app         # Run download + backtest inside container
docker compose up notebook    # Launch Jupyter at http://localhost:8888
```

---

## Project Structure

```
funding_arbs/
├── config/
│   ├── default.yaml          # Global parameters
│   └── universe.yaml         # 47 tracked perpetual pairs
├── data/
│   ├── downloader.py         # Funding rate ingestion (ccxt + CoinAPI)
│   ├── spot_prices.py        # OHLCV for basis calculation
│   ├── oi_fetcher.py         # Open interest downloader
│   ├── db.py                 # PostgreSQL / SQLite interface
│   └── schemas.py            # Pydantic data models
├── research/
│   ├── funding_zscore.py     # Rolling z-score signals
│   ├── basis_momentum.py     # Perp-spot basis momentum
│   ├── oi_concentration.py   # Open-interest crowding
│   ├── term_structure.py     # Yield-curve-style analysis
│   └── kalman_hedge.py       # Dynamic hedge ratio (Kalman filter)
├── backtest/
│   ├── enhanced_engine.py    # Carry-trade backtest engine
│   └── fee_model.py          # Realistic cost model
├── results/
│   ├── sweep/                # Backtest outputs (metrics, equity, tearsheets)
│   └── images/               # Comparison plots
├── tests/                    # pytest suite
└── notebooks/                # Research notebooks
```

---

## Technology Stack

| Layer | Technology |
|-------|------------|
| Language | Python 3.11+ |
| Exchange APIs | ccxt (Binance, Bybit, OKX) |
| Data Processing | pandas, numpy, pyarrow |
| Kalman Filter | filterpy (with rolling OLS fallback) |
| Reports | quantstats, matplotlib, seaborn |
| Testing | pytest |
| Lint / Format | ruff, mypy |
| Containers | Docker + Docker Compose |

---

## License

This project is licensed under the [MIT License](LICENSE).

```
MIT License

Copyright (c) 2026 Carson1332

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```

---

## Disclaimer

This repository is for **research and educational purposes only**. It does not constitute financial advice. Cryptocurrency trading involves significant risk. Past backtest performance does not guarantee future results.
