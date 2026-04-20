import json
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

# Setup style
plt.style.use("seaborn-v0_8-whitegrid")
plt.rcParams["font.family"] = "sans-serif"
plt.rcParams["figure.figsize"] = (12, 8)
plt.rcParams["axes.titlesize"] = 16
plt.rcParams["axes.labelsize"] = 14

out_dir = Path("results/sweep")
img_dir = Path("results/images")
img_dir.mkdir(parents=True, exist_ok=True)


def plot_equity_curves():
    fig, ax = plt.subplots(figsize=(14, 8))

    # Plot best unidirectional
    uni_file = out_dir / "equity_carry_moderate.csv"
    if uni_file.exists():
        df = pd.read_csv(uni_file)
        df["timestamp"] = pd.to_datetime(df["timestamp"], format="mixed")
        df = df.set_index("timestamp")
        df["return_pct"] = (df["equity"] / df["equity"].iloc[0] - 1) * 100
        ax.plot(
            df.index,
            df["return_pct"],
            linewidth=2.5,
            linestyle="--",
            color="steelblue",
            label="Unidirectional (carry_moderate)",
        )

    # Plot bidirectional variants
    bi_files = sorted(out_dir.glob("equity_bidirectional_*.csv"))
    colors = {"5borrow": "darkgreen", "8borrow": "forestgreen", "12borrow": "olivedrab"}
    for f in bi_files:
        name = f.stem.replace("equity_", "")
        borrow = name.replace("bidirectional_", "")
        df = pd.read_csv(f)
        df["timestamp"] = pd.to_datetime(df["timestamp"], format="mixed")
        df = df.set_index("timestamp")
        df["return_pct"] = (df["equity"] / df["equity"].iloc[0] - 1) * 100
        color = colors.get(borrow, "green")
        ax.plot(
            df.index,
            df["return_pct"],
            linewidth=2.5,
            color=color,
            label=f"Bidirectional ({borrow})",
        )

    ax.set_title("Strategy Evolution: Unidirectional vs Bidirectional Carry")
    ax.set_ylabel("Cumulative Return (%)")
    ax.set_xlabel("Date")
    ax.legend(loc="upper left")
    ax.grid(True, alpha=0.3)

    plt.tight_layout()
    plt.savefig(img_dir / "equity_comparison.png", dpi=300)
    plt.close()


def plot_drawdown():
    fig, ax = plt.subplots(figsize=(14, 6))

    files = [
        ("equity_carry_moderate.csv", "Unidirectional", "steelblue", "--"),
        ("equity_bidirectional_5borrow.csv", "Bidirectional 5% borrow", "darkgreen", "-"),
        ("equity_bidirectional_8borrow.csv", "Bidirectional 8% borrow", "forestgreen", "-"),
        ("equity_bidirectional_12borrow.csv", "Bidirectional 12% borrow", "olivedrab", "-"),
    ]

    for filename, label, color, linestyle in files:
        f = out_dir / filename
        if not f.exists():
            continue
        df = pd.read_csv(f)
        df["timestamp"] = pd.to_datetime(df["timestamp"], format="mixed")
        df = df.set_index("timestamp")
        peak = df["equity"].cummax()
        dd = (df["equity"] - peak) / peak * 100
        ax.plot(df.index, dd, linewidth=2, color=color, linestyle=linestyle, label=label)

    ax.set_title("Drawdown Comparison: Unidirectional vs Bidirectional")
    ax.set_ylabel("Drawdown (%)")
    ax.set_xlabel("Date")
    ax.axhline(y=-1, color="red", linestyle=":", alpha=0.5, label="1% DD")
    ax.legend(loc="lower left")
    ax.grid(True, alpha=0.3)

    plt.tight_layout()
    plt.savefig(img_dir / "drawdown_comparison.png", dpi=300)
    plt.close()


def plot_metrics_comparison():
    with open(out_dir / "all_metrics.json", "r") as f:
        metrics = json.load(f)

    df = pd.DataFrame(metrics).T

    # Select key strategies
    selected = [
        "carry_moderate",
        "bidirectional_5borrow",
        "bidirectional_8borrow",
        "bidirectional_12borrow",
    ]
    df = df.loc[[s for s in selected if s in df.index]]

    fig, axes = plt.subplots(2, 2, figsize=(16, 12))

    # Sharpe
    ax = axes[0, 0]
    sharpe = df["sharpe"].sort_values()
    colors = ["steelblue" if "carry" in i else "darkgreen" for i in sharpe.index]
    sharpe.plot(kind="barh", ax=ax, color=colors)
    ax.set_title("Sharpe Ratio")
    ax.set_xlabel("Sharpe")
    for i, v in enumerate(sharpe):
        ax.text(v + 0.05, i, f"{v:.2f}", va="center")

    # Annual Return
    ax = axes[0, 1]
    ret = df["annual_return_pct"].sort_values()
    colors = ["steelblue" if "carry" in i else "darkgreen" for i in ret.index]
    ret.plot(kind="barh", ax=ax, color=colors)
    ax.set_title("Annualised Return (%)")
    ax.set_xlabel("Return %")
    for i, v in enumerate(ret):
        ax.text(v + 0.05, i, f"{v:.2f}%", va="center")

    # Max Drawdown
    ax = axes[1, 0]
    dd = df["max_drawdown_pct"].sort_values(ascending=False)
    colors = ["steelblue" if "carry" in i else "darkgreen" for i in dd.index]
    dd.plot(kind="barh", ax=ax, color=colors)
    ax.set_title("Max Drawdown (%)")
    ax.set_xlabel("Max DD %")
    for i, v in enumerate(dd):
        ax.text(v - 0.05, i, f"{v:.2f}%", va="center", ha="right")

    # Win Rate
    ax = axes[1, 1]
    wr = df["win_rate_pct"].sort_values()
    colors = ["steelblue" if "carry" in i else "darkgreen" for i in wr.index]
    wr.plot(kind="barh", ax=ax, color=colors)
    ax.set_title("Win Rate (%)")
    ax.set_xlabel("Win Rate %")
    for i, v in enumerate(wr):
        ax.text(v + 0.5, i, f"{v:.1f}%", va="center")

    plt.suptitle("Metrics Comparison: Unidirectional vs Bidirectional Carry", fontsize=18, y=1.02)
    plt.tight_layout()
    plt.savefig(img_dir / "metrics_comparison.png", dpi=300)
    plt.close()


def plot_regime_sensitivity():
    """Plot equity curves with regime shading."""
    fig, ax = plt.subplots(figsize=(14, 8))

    # Plot bidirectional 5%
    df = pd.read_csv(out_dir / "equity_bidirectional_5borrow.csv")
    df["timestamp"] = pd.to_datetime(df["timestamp"], format="mixed")
    df = df.set_index("timestamp")
    df["return_pct"] = (df["equity"] / df["equity"].iloc[0] - 1) * 100
    ax.plot(
        df.index, df["return_pct"], linewidth=2.5,
        color="darkgreen", label="Bidirectional 5% borrow",
    )

    # Plot unidirectional
    df2 = pd.read_csv(out_dir / "equity_carry_moderate.csv")
    df2["timestamp"] = pd.to_datetime(df2["timestamp"], format="mixed")
    df2 = df2.set_index("timestamp")
    df2["return_pct"] = (df2["equity"] / df2["equity"].iloc[0] - 1) * 100
    ax.plot(
        df2.index, df2["return_pct"], linewidth=2.5,
        linestyle="--", color="steelblue", label="Unidirectional",
    )

    # Shade regimes
    ax.axvspan(
        pd.Timestamp("2024-01-01"), pd.Timestamp("2024-12-31"),
        alpha=0.05, color="green", label="2024: +11.9% avg funding",
    )
    ax.axvspan(
        pd.Timestamp("2025-01-01"), pd.Timestamp("2025-12-31"),
        alpha=0.05, color="gray", label="2025: +1.3% avg funding",
    )
    ax.axvspan(
        pd.Timestamp("2026-01-01"), pd.Timestamp("2026-04-20"),
        alpha=0.05, color="red", label="2026: -11.6% avg funding",
    )

    ax.set_title("Regime Sensitivity: Strategy Performance Across Funding Environments")
    ax.set_ylabel("Cumulative Return (%)")
    ax.set_xlabel("Date")
    ax.legend(loc="upper left")
    ax.grid(True, alpha=0.3)

    plt.tight_layout()
    plt.savefig(img_dir / "regime_sensitivity.png", dpi=300)
    plt.close()


if __name__ == "__main__":
    print("Generating plots...")
    plot_equity_curves()
    plot_drawdown()
    plot_metrics_comparison()
    plot_regime_sensitivity()
    print("Done! Saved to results/images/")
