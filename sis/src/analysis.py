"""Stage 6 - exploratory data analysis and visualisation (reads from SQLite, not from CSV).

Research question: is higher trading volume associated with higher price volatility for AAPL, MSFT, NVDA, AMZN, GOOGL?

Volatility proxies : abs_return (|daily log return|, %), intraday_range_pct ((high-low)/close, %), volatility_20d
Volume measures    : log_rel_volume (log of volume / mean volume of previous 20 days), volume_zscore
Outputs            : figures/*.png, data/eda_*.csv, docs/eda_findings.md
"""
from __future__ import annotations

import logging
import sqlite3
import sys

import matplotlib

matplotlib.use("Agg")  
import matplotlib.pyplot as plt  
import numpy as np  
import pandas as pd  
import seaborn as sns 
from scipy import stats  

from .config import DATA_DIR, DOCS_DIR, FIGURES_DIR, SETTINGS, ensure_dirs  
from .report import update_stage 

log = logging.getLogger("analysis")
sns.set_theme(style="whitegrid", context="notebook")
LAGS = range(-3, 4)


def load_data() -> pd.DataFrame:
    if not SETTINGS.db_path.exists():
        raise FileNotFoundError(f"{SETTINGS.db_path} not found - run the loader first")
    with sqlite3.connect(SETTINGS.db_path) as conn:
        df = pd.read_sql_query(
            "SELECT p.*, s.company_name, s.sector FROM daily_prices p JOIN symbols s USING(symbol) ORDER BY symbol, trade_date",
            conn, parse_dates=["trade_date"],
        )
    if df.empty:
        raise ValueError("daily_prices is empty")
    return df


def correlations(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for symbol, g in df.groupby("symbol"):
        for target in ("abs_return", "intraday_range_pct"):
            sub = g[["log_rel_volume", target]].dropna()
            pr, pp = stats.pearsonr(sub["log_rel_volume"], sub[target])
            sr, sp = stats.spearmanr(sub["log_rel_volume"], sub[target])
            rows.append({"symbol": symbol, "volatility_measure": target, "n": len(sub),
                         "pearson_r": pr, "pearson_p": pp, "spearman_rho": sr, "spearman_p": sp})
    return pd.DataFrame(rows)


def lagged_correlations(df: pd.DataFrame) -> pd.DataFrame:
    """corr(log_rel_volume at day t, intraday range at day t+k). k>0: volume leads, k<0: volatility leads."""
    out = {}
    for symbol, g in df.groupby("symbol"):
        g = g.sort_values("trade_date")
        out[symbol] = {k: g["log_rel_volume"].corr(g["intraday_range_pct"].shift(-k), method="spearman") for k in LAGS}
    return pd.DataFrame(out).T


def quintile_table(df: pd.DataFrame) -> pd.DataFrame:
    d = df.dropna(subset=["rel_volume", "intraday_range_pct", "abs_return"]).copy()
    d["volume_quintile"] = d.groupby("symbol")["rel_volume"].transform(
        lambda s: pd.qcut(s, 5, labels=["Q1 (lowest)", "Q2", "Q3", "Q4", "Q5 (highest)"])
    )
    return d


def make_figures(df: pd.DataFrame, corr: pd.DataFrame, lagged: pd.DataFrame, quint: pd.DataFrame) -> list[str]:
    paths = []

    g = sns.lmplot(data=df.dropna(subset=["log_rel_volume", "intraday_range_pct"]), x="log_rel_volume", y="intraday_range_pct",
                   col="symbol", col_wrap=3, height=3.4, scatter_kws={"alpha": 0.25, "s": 12},
                   line_kws={"color": "crimson"}, facet_kws={"sharex": False, "sharey": False})
    g.set_axis_labels("log(relative volume)  [0 = normal day]", "Intraday range (high-low)/close, %")
    g.set_titles("{col_name}")
    g.figure.suptitle("Fig. 1  Relative trading volume vs intraday price range", y=1.03)
    p = FIGURES_DIR / "fig1_volume_vs_range_scatter.png"
    g.savefig(p, dpi=150, bbox_inches="tight"); plt.close("all"); paths.append(p.name)

    fig, ax = plt.subplots(figsize=(9, 4.8))
    cdata = corr.melt(id_vars=["symbol", "volatility_measure"], value_vars=["pearson_r", "spearman_rho"], var_name="method", value_name="r")
    cdata["pair"] = cdata["volatility_measure"].map({"abs_return": "|return|", "intraday_range_pct": "intraday range"}) + " / " + \
        cdata["method"].map({"pearson_r": "Pearson", "spearman_rho": "Spearman"})
    sns.barplot(data=cdata, x="symbol", y="r", hue="pair", ax=ax)
    ax.set_title("Fig. 2  Correlation of relative volume with two volatility measures")
    ax.set_xlabel("Stock"); ax.set_ylabel("Correlation coefficient")
    ax.legend(title="Volatility measure / method", fontsize=8, loc="upper left", bbox_to_anchor=(1.01, 1))
    p = FIGURES_DIR / "fig2_correlation_by_stock.png"
    fig.savefig(p, dpi=150, bbox_inches="tight"); plt.close(fig); paths.append(p.name)

    fig, ax = plt.subplots(figsize=(9, 4.8))
    qmean = quint.groupby(["volume_quintile", "symbol"], observed=True)["intraday_range_pct"].mean().reset_index()
    sns.lineplot(data=qmean, x="volume_quintile", y="intraday_range_pct", hue="symbol", marker="o", ax=ax)
    ax.set_title("Fig. 3  Mean intraday range by relative-volume quintile (within each stock)")
    ax.set_xlabel("Relative-volume quintile"); ax.set_ylabel("Mean intraday range, %"); ax.legend(title="Stock")
    p = FIGURES_DIR / "fig3_volatility_by_volume_quintile.png"
    fig.savefig(p, dpi=150, bbox_inches="tight"); plt.close(fig); paths.append(p.name)

    symbols = sorted(df["symbol"].unique())
    fig, axes = plt.subplots(len(symbols), 1, figsize=(10, 2.6 * len(symbols)), sharex=True)
    axes = np.atleast_1d(axes)
    for ax, symbol in zip(axes, symbols):
        s = df[df["symbol"] == symbol].set_index("trade_date")
        vol20 = s["volume_zscore"].rolling(20, min_periods=20).mean()
        l1 = ax.plot(s.index, vol20, color="tab:blue", label="Volume z-score (20d avg)")
        ax2 = ax.twinx()
        l2 = ax2.plot(s.index, s["volatility_20d"], color="tab:red", label="Realised volatility 20d (% annualised)")
        ax.set_ylabel(symbol); ax2.grid(False)
        if symbol == symbols[0]:
            ax.legend(l1 + l2, [l.get_label() for l in l1 + l2], loc="upper left", fontsize=7)
    axes[0].set_title("Fig. 4  Rolling volume vs rolling realised volatility over time")
    axes[-1].set_xlabel("Date")
    p = FIGURES_DIR / "fig4_timeseries_volume_vs_volatility.png"
    fig.savefig(p, dpi=150, bbox_inches="tight"); plt.close(fig); paths.append(p.name)

    # Fig 5 - lead/lag heatmap
    fig, ax = plt.subplots(figsize=(8, 3.8))
    sns.heatmap(lagged, annot=True, fmt=".2f", cmap="vlag", center=0, ax=ax, cbar_kws={"label": "Spearman rho"})
    ax.set_title("Fig. 5  Volume(t) vs intraday range(t+k): k<0 volatility leads, k>0 volume leads")
    ax.set_xlabel("Lag k (trading days)"); ax.set_ylabel("Stock")
    p = FIGURES_DIR / "fig5_lead_lag_heatmap.png"
    fig.savefig(p, dpi=150, bbox_inches="tight"); plt.close(fig); paths.append(p.name)
    return paths


def write_findings(df, corr, lagged, quint, desc, figures) -> None:
    best = corr[corr["volatility_measure"] == "intraday_range_pct"].sort_values("spearman_rho", ascending=False)
    q = quint.groupby("volume_quintile", observed=True)["intraday_range_pct"].mean()
    lag0 = lagged[0]
    lines = [
        "# EDA findings (generated automatically from output.db)",
        "",
        f"- Observations: **{len(df)}** trading days, **{df['symbol'].nunique()}** stocks, "
        f"{df['trade_date'].min():%Y-%m-%d} to {df['trade_date'].max():%Y-%m-%d}.",
        f"- Volume vs intraday range (Spearman): strongest for **{best.iloc[0]['symbol']}** (rho = {best.iloc[0]['spearman_rho']:.2f}), "
        f"weakest for **{best.iloc[-1]['symbol']}** (rho = {best.iloc[-1]['spearman_rho']:.2f}). "
        f"All p-values < 0.05: **{bool((corr['spearman_p'] < 0.05).all())}**.",
        f"- Mean intraday range rises from **{q.iloc[0]:.2f}%** (lowest-volume quintile) to **{q.iloc[-1]:.2f}%** (highest-volume quintile).",
        f"- Same-day (k=0) Spearman correlation between volume and range: " + ", ".join(f"{s} {v:.2f}" for s, v in lag0.items()) + ".",
        f"- Lead/lag: same-day correlation is the largest in absolute terms for "
        f"{int((lagged.abs().idxmax(axis=1) == 0).sum())} of {len(lagged)} stocks.",
        "- Correlation is not causation: volume and volatility are both driven by news (earnings, macro releases).",
        "",
        "## Figures",
        *[f"- figures/{f}" for f in figures],
        "",
        "## Descriptive statistics",
        "```",
        desc.to_string(),
        "```",
    ]
    (DOCS_DIR / "eda_findings.md").write_text("\n".join(lines), encoding="utf-8")


def run() -> dict:
    ensure_dirs()
    df = load_data()

    print("\n=== EDA: dataset overview ===")
    print("shape:", df.shape)
    print(df.dtypes.to_string())
    num_cols = ["close", "volume", "abs_return", "intraday_range_pct", "rel_volume", "volatility_20d"]
    desc = df[num_cols].describe().round(3)
    print("\nDescriptive statistics:\n", desc.to_string())
    print("\nCategorical counts:\n", df["symbol"].value_counts().to_string(), "\n", df["direction"].value_counts(normalize=True).round(3).to_string())
    by_sym = df.groupby("symbol")[["volume", "abs_return", "intraday_range_pct", "volatility_20d"]].agg(["mean", "median", "std"]).round(3)
    print("\nGrouped statistics by symbol:\n", by_sym.to_string())
    print("\nUp-day share by sector:\n", df.groupby("sector")["direction"].apply(lambda s: (s == "up").mean()).round(3).to_string())

    corr = correlations(df)
    lagged = lagged_correlations(df)
    quint = quintile_table(df)
    print("\nCorrelations (relative volume vs volatility):\n", corr.round(4).to_string(index=False))
    print("\nLead/lag Spearman correlations:\n", lagged.round(3).to_string())

    corr.to_csv(DATA_DIR / "eda_correlations.csv", index=False)
    lagged.to_csv(DATA_DIR / "eda_lagged_correlations.csv")
    figures = make_figures(df, corr, lagged, quint)
    write_findings(df, corr, lagged, quint, desc, figures)

    result = {"rows_analysed": len(df), "figures": figures, "findings_file": "docs/eda_findings.md"}
    update_stage("analysis", result)
    print(f"\nSaved {len(figures)} figures to {FIGURES_DIR} and findings to docs/eda_findings.md")
    return result


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    try:
        run()
    except (FileNotFoundError, ValueError) as exc:
        log.error("%s", exc)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
