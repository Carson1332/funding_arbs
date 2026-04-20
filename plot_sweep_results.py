import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import json
from pathlib import Path

# Setup style
plt.style.use('seaborn-v0_8-whitegrid')
plt.rcParams['font.family'] = 'sans-serif'
plt.rcParams['figure.figsize'] = (12, 8)
plt.rcParams['axes.titlesize'] = 16
plt.rcParams['axes.labelsize'] = 14

out_dir = Path('results/sweep')
img_dir = Path('results/images')
img_dir.mkdir(parents=True, exist_ok=True)

def plot_equity_curves():
    fig, ax = plt.subplots(figsize=(14, 8))
    
    files = list(out_dir.glob('equity_carry_*.csv'))
    for f in files:
        df = pd.read_csv(f)
        df['timestamp'] = pd.to_datetime(df['timestamp'], format='mixed')
        df = df.set_index('timestamp')
        
        name = f.stem.replace('equity_', '')
        
        # Calculate percentage return
        df['return_pct'] = (df['equity'] / df['equity'].iloc[0] - 1) * 100
        
        # Plot with different line styles
        if 'concentrated' in name:
            ax.plot(df.index, df['return_pct'], linewidth=2.5, label=f"{name} (Concentrated)")
        elif 'diversified' in name:
            ax.plot(df.index, df['return_pct'], linewidth=2.5, linestyle='--', label=f"{name} (Diversified)")
        elif 'strict' in name and 'ts' not in name:
            ax.plot(df.index, df['return_pct'], linewidth=2, label=name)
        else:
            ax.plot(df.index, df['return_pct'], linewidth=1.5, alpha=0.7, label=name)
            
    ax.set_title('Funding Arbitrage Strategies: Cumulative Return (%)')
    ax.set_ylabel('Return (%)')
    ax.set_xlabel('Date')
    ax.legend(loc='upper left')
    ax.grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(img_dir / 'equity_curves.png', dpi=300)
    plt.close()

def plot_drawdown():
    fig, ax = plt.subplots(figsize=(14, 6))
    
    files = list(out_dir.glob('equity_carry_*.csv'))
    for f in files:
        if 'ts' in f.stem: continue # Skip TS variants for clarity
            
        df = pd.read_csv(f)
        df['timestamp'] = pd.to_datetime(df['timestamp'], format='mixed')
        df = df.set_index('timestamp')
        
        name = f.stem.replace('equity_', '')
        
        # Calculate drawdown
        peak = df['equity'].cummax()
        dd = (df['equity'] - peak) / peak * 100
        
        if 'concentrated' in name:
            ax.plot(df.index, dd, linewidth=2, label=name)
        elif 'diversified' in name:
            ax.plot(df.index, dd, linewidth=2, linestyle='--', label=name)
        else:
            ax.plot(df.index, dd, linewidth=1, alpha=0.5, label=name)
            
    ax.set_title('Strategy Drawdowns (%)')
    ax.set_ylabel('Drawdown (%)')
    ax.set_xlabel('Date')
    ax.fill_between(df.index, 0, -2, color='red', alpha=0.05) # Highlight 2% DD zone
    ax.legend(loc='lower left')
    ax.grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(img_dir / 'drawdowns.png', dpi=300)
    plt.close()

def plot_metrics_comparison():
    with open(out_dir / 'all_metrics.json', 'r') as f:
        metrics = json.load(f)
        
    df = pd.DataFrame(metrics).T
    
    # Filter only carry variants
    df = df[df.index.str.contains('carry')]
    
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))
    
    # Plot Sharpe
    sharpe = df['sharpe'].sort_values()
    colors = ['skyblue' if i < len(sharpe)-2 else 'royalblue' for i in range(len(sharpe))]
    sharpe.plot(kind='barh', ax=ax1, color=colors)
    ax1.set_title('Sharpe Ratio Comparison')
    ax1.set_xlabel('Sharpe Ratio')
    for i, v in enumerate(sharpe):
        ax1.text(v + 0.1, i, f"{v:.2f}", va='center')
        
    # Plot Win Rate
    win_rate = df['win_rate_pct'].sort_values()
    colors = ['lightgreen' if i < len(win_rate)-2 else 'forestgreen' for i in range(len(win_rate))]
    win_rate.plot(kind='barh', ax=ax2, color=colors)
    ax2.set_title('Win Rate (%) Comparison')
    ax2.set_xlabel('Win Rate (%)')
    for i, v in enumerate(win_rate):
        ax2.text(v + 1, i, f"{v:.1f}%", va='center')
        
    plt.tight_layout()
    plt.savefig(img_dir / 'metrics_comparison.png', dpi=300)
    plt.close()

if __name__ == "__main__":
    print("Generating plots...")
    plot_equity_curves()
    plot_drawdown()
    plot_metrics_comparison()
    print("Done!")
