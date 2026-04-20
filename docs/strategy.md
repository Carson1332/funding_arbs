# Strategy Description

## Overview

This project implements a **delta-neutral funding rate arbitrage** strategy across cryptocurrency perpetual futures markets. The core idea is straightforward: when perpetual futures funding rates are persistently positive, a trader can earn yield by going long spot and short perpetual, collecting the funding payments while remaining market-neutral.

## How Funding Rates Work

Perpetual futures contracts have no expiry date. To keep the perpetual price anchored to the spot price, exchanges use a **funding rate mechanism**: every 8 hours, traders on one side of the market pay traders on the other side. When the funding rate is positive, longs pay shorts. When negative, shorts pay longs.

In practice, funding rates are positive the majority of the time because the crypto market has a structural long bias — more participants want leveraged long exposure than short exposure. This creates a persistent yield opportunity for those willing to take the other side.

## The Delta-Neutral Position

The strategy constructs a delta-neutral position by simultaneously holding two offsetting positions. The first is a **long spot** position, which involves buying the underlying cryptocurrency on the spot market. The second is a **short perpetual** position, which involves opening a short position on the perpetual futures contract for the same asset.

Because the spot and perpetual prices track each other closely, the combined position has near-zero directional exposure. The trader is not betting on whether BTC goes up or down — they are harvesting the funding rate payments that flow from longs to shorts.

## Signal Generation

Rather than blindly entering every positive-funding pair, the strategy uses multiple signals to identify the most attractive opportunities and manage risk.

**Funding Rate Z-Score** measures how extreme the current funding rate is relative to its recent history. A z-score above 1.5 indicates the rate is significantly elevated, suggesting a good entry point. The z-score also helps avoid entering when rates are only marginally positive.

**Basis Momentum** tracks the speed at which the perp-spot spread is changing. A rapidly widening basis often precedes funding rate spikes, providing an early entry signal. Conversely, a contracting basis warns that funding may be about to decline.

**Open Interest Concentration** monitors the buildup of speculative positions. When OI grows rapidly alongside high funding rates, it signals a crowded trade. The strategy reduces exposure in crowded conditions to avoid being caught in a liquidation cascade.

**Term Structure Analysis** examines funding rates across different time horizons (1-day, 7-day, 30-day averages). An "inverted" term structure — where short-term rates are below long-term rates — suggests the funding opportunity may be fading.

## Dynamic Hedge Ratio

A key innovation in this implementation is the use of a **Kalman filter** to estimate the time-varying hedge ratio between spot and perpetual prices. The naive approach assumes a 1:1 hedge (1 unit of spot offsets 1 unit of perp), but in reality the relationship drifts over time due to basis movements, funding accrual, and market microstructure effects.

The Kalman filter treats the true hedge ratio as a hidden state variable and estimates it from noisy price observations. This produces a smoother, more accurate hedge that reduces the P&L variance from basis drift, allowing the strategy to isolate the funding rate component more cleanly.

## Risk Management

The strategy employs several risk management mechanisms. A **portfolio-level drawdown limit** (default 10%) triggers a full exit of all positions if the portfolio drawdown exceeds the threshold. A **per-position loss limit** (default 2% of equity) exits individual positions that are losing money, which can happen if the basis moves adversely. **Position sizing** limits each position to a maximum percentage of equity (default 15%), and the total number of concurrent positions is capped (default 10). The **hedge rebalancing threshold** (default 3%) triggers a rebalance of the perpetual position when the Kalman-estimated hedge ratio drifts too far from the current position.

## Expected Performance Characteristics

This strategy is designed to produce **low-volatility, positive-carry returns** — similar in character to a fixed-income strategy rather than a directional equity strategy. The expected Sharpe ratio is in the range of 2.0 to 4.0, with annualised returns of 10 to 25% and maximum drawdowns under 5%. The primary risk is a sudden, sustained shift to negative funding rates, which would cause the strategy to pay rather than collect funding.
