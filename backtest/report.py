"""
Backtest Report Generator.

Generates:
1. results/metrics.json — machine-readable metrics (for CI badges)
2. results/equity_curve.csv — equity curve time series
3. results/tearsheet.html — full QuantStats HTML report
4. results/trade_log.csv — detailed trade log
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

try:
    import quantstats as qs
    HAS_QUANTSTATS = True
except ImportError:
    HAS_QUANTSTATS = False

from backtest.portfolio import PortfolioState


class BacktestReport:
    """
    Generate comprehensive backtest reports from portfolio state.
    """

    def __init__(self, state: PortfolioState):
        self.state = state
        self.equity = state.to_equity_df()
        self.returns = state.to_returns()
        self.trade_log = state.to_trade_log_df()

    def metrics(self) -> dict:
        """
        Compute summary metrics for the backtest.

        Returns dict compatible with CI badge extraction.
        """
        if self.returns.empty or len(self.returns) < 2:
            return {
                "sharpe": 0.0,
                "annual_return_pct": 0.0,
                "max_drawdown_pct": 0.0,
                "sortino": 0.0,
                "calmar": 0.0,
                "total_trades": 0,
                "backtest_start": "",
                "backtest_end": "",
            }

        returns = self.returns.copy()

        if HAS_QUANTSTATS:
            sharpe = qs.stats.sharpe(returns)
            annual_return = qs.stats.cagr(returns)
            max_dd = qs.stats.max_drawdown(returns)
            sortino = qs.stats.sortino(returns)
            calmar = qs.stats.calmar(returns)
        else:
            # Fallback manual calculations
            sharpe = self._manual_sharpe(returns)
            annual_return = self._manual_cagr(returns)
            max_dd = self._manual_max_drawdown(returns)
            sortino = self._manual_sortino(returns)
            calmar = annual_return / abs(max_dd) if max_dd != 0 else 0.0

        # Trade statistics
        total_trades = len(self.trade_log) if not self.trade_log.empty else 0
        winning_trades = 0
        if not self.trade_log.empty and "net_pnl" in self.trade_log.columns:
            closes = self.trade_log[self.trade_log["action"] == "CLOSE"]
            winning_trades = (closes["net_pnl"] > 0).sum()
            total_closes = len(closes)
            win_rate = winning_trades / total_closes if total_closes > 0 else 0
        else:
            win_rate = 0

        return {
            "sharpe": round(float(sharpe), 2) if not np.isnan(sharpe) else 0.0,
            "annual_return_pct": round(float(annual_return) * 100, 1) if not np.isnan(annual_return) else 0.0,
            "max_drawdown_pct": round(float(max_dd) * 100, 1) if not np.isnan(max_dd) else 0.0,
            "sortino": round(float(sortino), 2) if not np.isnan(sortino) else 0.0,
            "calmar": round(float(calmar), 2) if not np.isnan(calmar) else 0.0,
            "total_trades": total_trades,
            "win_rate": round(float(win_rate) * 100, 1),
            "backtest_start": str(self.equity.index[0]) if len(self.equity) > 0 else "",
            "backtest_end": str(self.equity.index[-1]) if len(self.equity) > 0 else "",
            "final_equity": round(float(self.state.equity), 2),
            "initial_capital": round(float(self.state.initial_capital), 2),
        }

    def save(self, output_dir: str = "results") -> None:
        """
        Save all report artifacts.

        Creates:
            {output_dir}/metrics.json
            {output_dir}/equity_curve.csv
            {output_dir}/tearsheet.html
            {output_dir}/trade_log.csv
        """
        out = Path(output_dir)
        out.mkdir(parents=True, exist_ok=True)

        # 1. JSON metrics (for CI badge)
        metrics = self.metrics()
        with open(out / "metrics.json", "w") as f:
            json.dump(metrics, f, indent=2)
        print(f"Saved metrics.json: Sharpe={metrics['sharpe']}, "
              f"Return={metrics['annual_return_pct']}%, "
              f"MaxDD={metrics['max_drawdown_pct']}%")

        # 2. Equity curve CSV
        if not self.equity.empty:
            self.equity.to_csv(out / "equity_curve.csv")
            print(f"Saved equity_curve.csv ({len(self.equity)} rows)")

        # 3. Full HTML tearsheet
        if HAS_QUANTSTATS and not self.returns.empty and len(self.returns) > 2:
            try:
                qs.reports.html(
                    self.returns,
                    output=str(out / "tearsheet.html"),
                    title="funding-arb: Delta-Neutral Funding Rate Arbitrage",
                )
                print("Saved tearsheet.html")
            except Exception as e:
                print(f"Warning: Could not generate QuantStats tearsheet: {e}")
                self._save_simple_tearsheet(out / "tearsheet.html", metrics)
        else:
            self._save_simple_tearsheet(out / "tearsheet.html", metrics)

        # 4. Trade log
        if not self.trade_log.empty:
            self.trade_log.to_csv(out / "trade_log.csv", index=False)
            print(f"Saved trade_log.csv ({len(self.trade_log)} trades)")

    def _save_simple_tearsheet(self, path: Path, metrics: dict) -> None:
        """Generate a simple HTML tearsheet when QuantStats is not available."""
        html = f"""<!DOCTYPE html>
<html>
<head><title>Funding Arb Backtest Report</title>
<style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, sans-serif; max-width: 800px; margin: 40px auto; padding: 0 20px; }}
    h1 {{ color: #1a1a2e; }}
    table {{ border-collapse: collapse; width: 100%; margin: 20px 0; }}
    th, td {{ border: 1px solid #ddd; padding: 12px; text-align: left; }}
    th {{ background-color: #1a1a2e; color: white; }}
    .positive {{ color: #0e8a16; font-weight: bold; }}
    .negative {{ color: #d73a49; font-weight: bold; }}
</style>
</head>
<body>
<h1>Funding Rate Arbitrage — Backtest Report</h1>
<table>
    <tr><th>Metric</th><th>Value</th></tr>
    <tr><td>Sharpe Ratio</td><td class="positive">{metrics['sharpe']}</td></tr>
    <tr><td>Annual Return</td><td class="positive">{metrics['annual_return_pct']}%</td></tr>
    <tr><td>Max Drawdown</td><td class="negative">{metrics['max_drawdown_pct']}%</td></tr>
    <tr><td>Sortino Ratio</td><td>{metrics['sortino']}</td></tr>
    <tr><td>Calmar Ratio</td><td>{metrics['calmar']}</td></tr>
    <tr><td>Total Trades</td><td>{metrics['total_trades']}</td></tr>
    <tr><td>Win Rate</td><td>{metrics.get('win_rate', 'N/A')}%</td></tr>
    <tr><td>Period</td><td>{metrics['backtest_start']} → {metrics['backtest_end']}</td></tr>
    <tr><td>Final Equity</td><td>${metrics.get('final_equity', 'N/A'):,.2f}</td></tr>
</table>
<p><em>Generated by funding-arb backtest engine. Install quantstats for full tearsheet.</em></p>
</body>
</html>"""
        with open(path, "w") as f:
            f.write(html)
        print("Saved simple tearsheet.html (install quantstats for full report)")

    # --- Manual metric calculations (fallback) ---

    @staticmethod
    def _manual_sharpe(returns: pd.Series, risk_free: float = 0.0, periods: int = 1095) -> float:
        """Sharpe ratio. periods=1095 for 8h intervals (3 per day * 365)."""
        excess = returns - risk_free / periods
        if excess.std() == 0:
            return 0.0
        return float(np.sqrt(periods) * excess.mean() / excess.std())

    @staticmethod
    def _manual_cagr(returns: pd.Series) -> float:
        """Compound annual growth rate."""
        total_return = (1 + returns).prod()
        n_years = len(returns) / 1095  # 8h periods per year
        if n_years <= 0 or total_return <= 0:
            return 0.0
        return float(total_return ** (1 / n_years) - 1)

    @staticmethod
    def _manual_max_drawdown(returns: pd.Series) -> float:
        """Maximum drawdown."""
        cumulative = (1 + returns).cumprod()
        peak = cumulative.cummax()
        drawdown = (cumulative - peak) / peak
        return float(drawdown.min())

    @staticmethod
    def _manual_sortino(returns: pd.Series, risk_free: float = 0.0, periods: int = 1095) -> float:
        """Sortino ratio."""
        excess = returns - risk_free / periods
        downside = excess[excess < 0]
        if len(downside) == 0 or downside.std() == 0:
            return 0.0
        return float(np.sqrt(periods) * excess.mean() / downside.std())
