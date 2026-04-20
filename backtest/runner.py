"""
Backtest Runner — Orchestrates the full backtest pipeline.

Ties together:
1. Data loading (from cache or download)
2. Signal generation (z-score, basis momentum, OI)
3. Simulation (funding rate replay)
4. Report generation (metrics, tearsheet)

Usage:
    python -m backtest.runner --config config/default.yaml
"""

from __future__ import annotations

import argparse
import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import yaml

from backtest.report import BacktestReport
from backtest.simulator import FundingRateSimulator, SimulatorConfig
from data.downloader import FundingRateDownloader
from research.funding_zscore import FundingZScore, ZScoreParams


def load_config(config_path: str) -> dict:
    """Load YAML configuration (default + universe)."""
    with open(config_path) as f:
        config = yaml.safe_load(f)

    # Also load universe.yaml if it exists next to the config file
    config_dir = Path(config_path).resolve().parent
    universe_path = config_dir / "universe.yaml"
    if universe_path.exists():
        with open(universe_path) as f:
            universe_config = yaml.safe_load(f)
            if universe_config and "universe" in universe_config:
                config["universe"] = universe_config["universe"]

    return config


def load_or_download_data(config: dict) -> pd.DataFrame:
    """Load cached data or download if not available."""
    # Try loading from cache first
    cached = FundingRateDownloader.load_cached_data()
    if len(cached) > 0:
        print(f"Loaded {len(cached)} cached funding rate records")
        return cached

    # Download if no cache
    print("No cached data found, downloading...")
    since_str = config.get("data_start", "2024-01-01")
    since = datetime.strptime(since_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    dl = FundingRateDownloader(config)
    return dl.fetch_all(since)


def generate_signals(funding_df: pd.DataFrame, config: dict) -> pd.DataFrame:
    """Generate trading signals from funding rate data."""
    zscore_params = ZScoreParams(
        lookback_periods=config.get("zscore_lookback", 90),
        entry_threshold=config.get("zscore_entry", 1.5),
        exit_threshold=config.get("zscore_exit", 0.0),
        min_annualised_rate=config.get("min_annualised_rate", 0.05),
    )

    zscore = FundingZScore(zscore_params)
    signals = zscore.compute(funding_df)
    return signals


def run_backtest(config: dict) -> None:
    """Run the full backtest pipeline."""
    print("=" * 60)
    print("FUNDING RATE ARBITRAGE BACKTEST")
    print("=" * 60)

    # Step 1: Load data
    print("\n[1/4] Loading data...")
    funding_df = load_or_download_data(config)
    if funding_df.empty:
        print("ERROR: No funding rate data available. Run data.downloader first.")
        return

    print(f"  Records: {len(funding_df)}")
    print(f"  Symbols: {funding_df['symbol'].nunique()}")
    print(f"  Exchanges: {funding_df['exchange'].nunique()}")
    print(f"  Date range: {funding_df['timestamp'].min()} -> {funding_df['timestamp'].max()}")

    # Step 2: Generate signals
    print("\n[2/4] Generating signals...")
    signals_df = generate_signals(funding_df, config)
    entry_signals = signals_df[signals_df["signal"] == 1]
    print(f"  Total signals: {len(signals_df)}")
    print(f"  Entry signals: {len(entry_signals)}")

    # Step 3: Run simulation
    print("\n[3/4] Running simulation...")
    sim_config = SimulatorConfig(
        initial_capital=config.get("initial_capital", 100_000),
        max_positions=config.get("max_positions", 10),
        max_position_pct=config.get("max_position_pct", 0.15),
        entry_zscore=config.get("zscore_entry", 1.5),
        exit_zscore=config.get("zscore_exit", 0.0),
        min_annualised_rate=config.get("min_annualised_rate", 0.05),
        use_kalman_hedge=config.get("use_kalman_hedge", True),
        exchange=config.get("primary_exchange", "binance"),
    )

    simulator = FundingRateSimulator(sim_config)
    state = simulator.run(funding_df, signals_df)

    # Step 4: Generate report
    print("\n[4/4] Generating report...")
    report = BacktestReport(state)
    output_dir = config.get("output_dir", "results")
    report.save(output_dir)

    # Print summary
    metrics = report.metrics()
    print("\n" + "=" * 60)
    print("BACKTEST RESULTS")
    print("=" * 60)
    print(f"  Sharpe Ratio:    {metrics['sharpe']}")
    print(f"  Annual Return:   {metrics['annual_return_pct']}%")
    print(f"  Max Drawdown:    {metrics['max_drawdown_pct']}%")
    print(f"  Sortino Ratio:   {metrics['sortino']}")
    print(f"  Calmar Ratio:    {metrics['calmar']}")
    print(f"  Total Trades:    {metrics['total_trades']}")
    print(f"  Win Rate:        {metrics.get('win_rate', 'N/A')}%")
    print(f"  Final Equity:    ${metrics.get('final_equity', 0):,.2f}")
    print(f"\n  Results saved to: {output_dir}/")
    print(f"  View tearsheet:   {output_dir}/tearsheet.html")


def main():
    parser = argparse.ArgumentParser(description="Run funding rate arbitrage backtest")
    parser.add_argument(
        "--config",
        type=str,
        default="config/default.yaml",
        help="Path to config YAML file",
    )
    args = parser.parse_args()

    config = load_config(args.config)
    run_backtest(config)


if __name__ == "__main__":
    main()
