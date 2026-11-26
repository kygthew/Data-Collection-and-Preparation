"""Stage 4 - cleaning and transformation with pandas + NumPy.

Input : data/valid_data.csv  (output of the Pandera stage)
Output: data/clean_data.csv            - the table that is loaded into SQLite
        data/summary_by_symbol.csv     - GroupBy / aggregation result
        data/pivot_volume_by_month.csv - pivot_table result (reshaping)
        data/pivot_volume_long.csv     - melt result (reshaping back to long format)
        data/high_activity_days.csv    - multi-condition filter result

Why each step is needed is documented next to the code and in README.md / docs/.
"""
from __future__ import annotations

import logging
import sys

import numpy as np
import pandas as pd

from .config import COMPANIES, SETTINGS, ensure_dirs
from .report import update_stage

log = logging.getLogger("cleaner")

TRADING_DAYS = 252
ROLL = 20                      
SPLIT_SUSPECT_LOG_RETURN = 0.35  


def clean(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    stats: dict = {"input_rows": len(df)}

    
    for col in ("change", "changePercent", "vwap"):
        if col not in df.columns:
            df[col] = np.nan
    stats["missing_before"] = {c: int(n) for c, n in df.isna().sum().items() if n}
    df["change"] = df["change"].fillna(df["close"] - df["open"])
    df["changePercent"] = df["changePercent"].fillna((df["close"] - df["open"]) / df["open"] * 100)
    df["vwap"] = df["vwap"].fillna((df["high"] + df["low"] + df["close"]) / 3)
    stats["missing_after"] = {c: int(n) for c, n in df.isna().sum().items() if n}

    
    before = len(df)
    df = df.drop_duplicates(subset=["symbol", "date"], keep="last")
    stats["duplicates_removed"] = before - len(df)

    df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d")
    for col in ("open", "high", "low", "close", "vwap", "change", "changePercent"):
        df[col] = df[col].astype("float64")
    df["volume"] = df["volume"].astype("int64")

    df["symbol"] = df["symbol"].astype(str).str.strip().str.upper()
    df["company_name"] = df["symbol"].map(lambda s: COMPANIES[s][0])
    df["sector"] = df["symbol"].map(lambda s: COMPANIES[s][1])

 
    before = len(df)
    keep = (df["volume"] > 0) & (df["close"] > 0) & (df["high"] >= df["low"]) & (df["date"].dt.dayofweek < 5)
    df = df[keep].sort_values(["symbol", "date"]).reset_index(drop=True)
    stats["filtered_out"] = before - len(df)

    g = df.groupby("symbol", sort=False)
    prev_close = g["close"].shift(1)
    df["log_return"] = np.log(df["close"] / prev_close)                         
    suspect = df["log_return"].abs() > SPLIT_SUSPECT_LOG_RETURN
    df.loc[suspect, "log_return"] = np.nan                                       
    stats["split_suspect_returns_nulled"] = int(suspect.sum())
    df["abs_return"] = np.abs(df["log_return"]) * 100                            
    df["intraday_range_pct"] = (df["high"] - df["low"]) / df["close"] * 100     
    df["log_volume"] = np.log(df["volume"].to_numpy(dtype="float64"))
    
    avg_prev_volume = g["volume"].transform(lambda s: s.shift(1).rolling(ROLL, min_periods=ROLL).mean())
    df["rel_volume"] = df["volume"] / avg_prev_volume
    df["log_rel_volume"] = np.log(df["rel_volume"].to_numpy(dtype="float64"))
    df["volume_zscore"] = g["log_volume"].transform(lambda s: (s - s.mean()) / s.std(ddof=0))
    df["volatility_20d"] = (
        g["log_return"].transform(lambda s: s.rolling(ROLL, min_periods=ROLL).std()) * np.sqrt(TRADING_DAYS) * 100
    )
    df["direction"] = np.select([df["change"] > 0, df["change"] < 0], ["up", "down"], default="flat")
    df = df.replace([np.inf, -np.inf], np.nan)

    stats["output_rows"] = len(df)
    return df, stats


def aggregate_and_reshape(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """GroupBy + aggregation, pivot_table, melt, and a multi-condition filter."""
    summary = (
        df.groupby("symbol")
        .agg(
            trading_days=("date", "count"),
            first_date=("date", "min"),
            last_date=("date", "max"),
            mean_volume=("volume", "mean"),
            median_volume=("volume", "median"),
            mean_abs_return_pct=("abs_return", "mean"),
            mean_range_pct=("intraday_range_pct", "mean"),
            mean_volatility_20d=("volatility_20d", "mean"),
            up_days=("direction", lambda s: int((s == "up").sum())),
        )
        .round(4)
        .reset_index()
    )

    monthly = df.assign(month=df["date"].dt.to_period("M").astype(str))
    pivot = monthly.pivot_table(index="month", columns="symbol", values="volume", aggfunc="mean").round(0)
    long = pivot.reset_index().melt(id_vars="month", var_name="symbol", value_name="mean_daily_volume")

    high_activity = df[(df["rel_volume"] > 2) & (df["abs_return"] > 2) & (df["direction"] != "flat")]
    return {"summary": summary, "pivot": pivot.reset_index(), "long": long, "high_activity": high_activity}


def run() -> dict:
    ensure_dirs()
    if not SETTINGS.valid_csv.exists():
        raise FileNotFoundError(f"{SETTINGS.valid_csv} not found - run the validator first")
    df = pd.read_csv(SETTINGS.valid_csv)
    cleaned, stats = clean(df)
    outputs = aggregate_and_reshape(cleaned)

    cleaned.assign(date=cleaned["date"].dt.strftime("%Y-%m-%d")).to_csv(SETTINGS.clean_csv, index=False)
    outputs["summary"].to_csv(SETTINGS.clean_csv.with_name("summary_by_symbol.csv"), index=False)
    outputs["pivot"].to_csv(SETTINGS.clean_csv.with_name("pivot_volume_by_month.csv"), index=False)
    outputs["long"].to_csv(SETTINGS.clean_csv.with_name("pivot_volume_long.csv"), index=False)
    outputs["high_activity"].to_csv(SETTINGS.clean_csv.with_name("high_activity_days.csv"), index=False)

    stats["high_activity_days"] = len(outputs["high_activity"])
    stats["pivot_shape"] = list(outputs["pivot"].shape)
    update_stage("clean", stats)

    print("\n=== Cleaning & transformation ===")
    for k, v in stats.items():
        print(f"{k:32s} {v}")
    print("\nGroupBy summary:\n", outputs["summary"].to_string(index=False))
    print("\nPivot table (last 5 months):\n", outputs["pivot"].tail().to_string(index=False))
    return stats


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    try:
        run()
    except FileNotFoundError as exc:
        log.error("%s", exc)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
