"""Generate QuantStats HTML tearsheets for sweep results."""
from __future__ import annotations

from pathlib import Path

import pandas as pd
import quantstats as qs


def generate_tearsheets():
    sweep_dir = Path("results/sweep")
    output_dir = sweep_dir

    files = sorted(sweep_dir.glob("equity_carry_*.csv"))
    for f in files:
        name = f.stem.replace("equity_", "")
        df = pd.read_csv(f)
        df["timestamp"] = pd.to_datetime(df["timestamp"], format="mixed")
        df = df.set_index("timestamp").sort_index()

        if df.empty or len(df) < 3:
            continue

        returns = df["equity"].pct_change().dropna()
        if returns.empty:
            continue

        output_path = output_dir / f"tearsheet_{name}.html"
        try:
            qs.reports.html(
                returns,
                output=str(output_path),
                title=f"funding-arb: {name}",
                periods_per_year=1095,
            )
            print(f"Saved {output_path}")
        except Exception as e:
            print(f"Warning: could not generate tearsheet for {name}: {e}")


if __name__ == "__main__":
    generate_tearsheets()
