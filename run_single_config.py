"""
Run a single backtest configuration.

Usage:
    python run_single_config.py --name carry_strict
    python run_single_config.py --name carry_moderate
    python run_single_config.py --name carry_relaxed
    python run_single_config.py --name carry_ts_strict
    python run_single_config.py --name carry_ts_moderate
    python run_single_config.py --name carry_concentrated
    python run_single_config.py --name carry_diversified
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))

from backtest.enhanced_engine import load_all_funding_data
from run_parameter_sweep import SweepBacktest, SweepConfig, compute_metrics

CONFIGS = {
    "carry_strict": {
        "strategy": "adaptive_carry_v3",
        "min_ann_rate_entry": 0.10,
        "min_positive_streak": 9,
        "exit_ann_rate": 0.03,
        "max_hold_periods": 180,
    },
    "carry_moderate": {
        "strategy": "adaptive_carry_v3",
        "min_ann_rate_entry": 0.08,
        "min_positive_streak": 6,
        "exit_ann_rate": 0.02,
        "max_hold_periods": 180,
    },
    "carry_relaxed": {
        "strategy": "adaptive_carry_v3",
        "min_ann_rate_entry": 0.06,
        "min_positive_streak": 3,
        "exit_ann_rate": 0.01,
        "max_hold_periods": 270,
    },
    "carry_ts_strict": {
        "strategy": "carry_plus_ts",
        "min_ann_rate_entry": 0.08,
    },
    "carry_ts_moderate": {
        "strategy": "carry_plus_ts",
        "min_ann_rate_entry": 0.06,
    },
    "carry_concentrated": {
        "strategy": "adaptive_carry_v3",
        "max_positions": 5,
        "max_position_pct": 0.30,
        "min_ann_rate_entry": 0.10,
        "min_positive_streak": 9,
    },
    "carry_diversified": {
        "strategy": "adaptive_carry_v3",
        "max_positions": 12,
        "max_position_pct": 0.12,
        "min_ann_rate_entry": 0.08,
        "min_positive_streak": 6,
    },
}


def run_single(name: str) -> dict:
    """Run a single configuration and save results."""
    if name not in CONFIGS:
        raise ValueError(f"Unknown config: {name}. Available: {list(CONFIGS.keys())}")

    print(f"\n{'='*60}")
    print(f"Running: {name}")
    print(f"{'='*60}")

    df = load_all_funding_data()
    config = SweepConfig(**CONFIGS[name])
    bt = SweepBacktest(config)
    state = bt.run(df)
    metrics = compute_metrics(state)

    out_dir = Path("results/sweep")
    out_dir.mkdir(parents=True, exist_ok=True)

    # Save equity
    eq_df = pd.DataFrame(state.equity_history)
    eq_df.to_csv(out_dir / f"equity_{name}.csv", index=False)

    # Save trades
    if state.trade_log:
        pd.DataFrame(state.trade_log).to_csv(out_dir / f"trades_{name}.csv", index=False)

    # Save individual metrics
    with open(out_dir / f"metrics_{name}.json", "w") as f:
        json.dump(metrics, f, indent=2, default=str)

    print(f"  Sharpe: {metrics.get('sharpe')} | Return: {metrics.get('annual_return_pct')}% | "
          f"MaxDD: {metrics.get('max_drawdown_pct')}% | WinRate: {metrics.get('win_rate_pct')}% | "
          f"PF: {metrics.get('profit_factor')} | Final: ${metrics.get('final_equity', 0):,.0f}")

    return metrics


def build_summary():
    """Build summary from all individual metrics files."""
    out_dir = Path("results/sweep")
    all_results = {}

    for name in CONFIGS.keys():
        metrics_file = out_dir / f"metrics_{name}.json"
        if metrics_file.exists():
            with open(metrics_file) as f:
                all_results[name] = json.load(f)

    if not all_results:
        print("No metrics files found. Run individual configs first.")
        return

    summary = pd.DataFrame(all_results).T
    cols = ["sharpe", "annual_return_pct", "max_drawdown_pct", "sortino", "calmar",
            "win_rate_pct", "total_trades", "profit_factor", "avg_hold_days", "final_equity"]
    available = [c for c in cols if c in summary.columns]
    print(f"\n{'='*60}")
    print("STRATEGY COMPARISON")
    print(f"{'='*60}")
    print(summary[available].to_string())

    summary.to_csv(out_dir / "sweep_comparison.csv")
    with open(out_dir / "all_metrics.json", "w") as f:
        json.dump(all_results, f, indent=2, default=str)

    print(f"\nSummary saved to {out_dir}/sweep_comparison.csv and all_metrics.json")


def main():
    parser = argparse.ArgumentParser(description="Run single backtest config")
    parser.add_argument("--name", type=str, default=None, help="Config name to run")
    parser.add_argument(
        "--summary", action="store_true", help="Build summary from existing results"
    )
    args = parser.parse_args()

    if args.summary:
        build_summary()
    elif args.name:
        run_single(args.name)
    else:
        parser.error("Either --name or --summary is required")


if __name__ == "__main__":
    main()
