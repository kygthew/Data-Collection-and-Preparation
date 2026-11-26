"""Stage 3 - data validation with Pandera.

Input : data/consumed_data.jsonl   (what actually came out of Kafka)
Output: data/valid_data.csv        (records that passed every rule, test records excluded)
        data/invalid_records.csv   (rejected records + the rules they broke)

Validation rules
  R1  types          open/high/low/close/volume must be numeric (checked per cell), volume must be a whole number
  R2  required       symbol, date, open, high, low, close, volume must not be null
  R3  allowed values symbol must be one of the configured tickers
  R4  date           date must be a valid YYYY-MM-DD calendar date, not in the future
  R5  ranges         prices > 0, volume >= 0
  R6  consistency    high >= low, high >= open/close, low <= open/close (row-wise, dataframe-level checks)
  R7  uniqueness     (symbol, date) must be unique (duplicates after the first are rejected)
"""
from __future__ import annotations

import logging
import sys

import pandas as pd

try:  # pandera >= 0.24
    import pandera.pandas as pa
except ImportError:  # older versions
    import pandera as pa

from .config import COMPANIES, SETTINGS, ensure_dirs
from .report import update_stage

log = logging.getLogger("validator")

PRICE_COLS = ["open", "high", "low", "close"]
REQUIRED_COLS = ["symbol", "date", *PRICE_COLS, "volume"]


def _is_valid_date(series: pd.Series) -> pd.Series:
    parsed = pd.to_datetime(series, format="%Y-%m-%d", errors="coerce")
    return parsed.notna() & (parsed <= pd.Timestamp.today().normalize())


def _describe(column, check: str) -> str:
    """Human-readable rule name for one failure (row-wise rules are not tied to a single column)."""
    if check == "not_nullable":
        return f"R2 missing value in '{column}'"
    if "uniqueness" in check:
        return "R7 duplicate (symbol, date)"
    if check.startswith("coerce_dtype"):
        return f"R1 wrong data type in '{column}'"
    if check.startswith("dtype"):
        return f"R1 wrong dtype in '{column}'"
    if check.startswith("R6"):
        return check
    return check


def build_schema(allowed_symbols: list[str]) -> pa.DataFrameSchema:
    return pa.DataFrameSchema(
        columns={
            "symbol": pa.Column(str, nullable=False, checks=pa.Check.isin(allowed_symbols, error="R3 symbol not in allowed list")),
            "date": pa.Column(str, nullable=False, checks=pa.Check(_is_valid_date, error="R4 invalid or future date")),
            "open": pa.Column(float, nullable=False, checks=pa.Check.gt(0, error="R5 open must be > 0")),
            "high": pa.Column(float, nullable=False, checks=pa.Check.gt(0, error="R5 high must be > 0")),
            "low": pa.Column(float, nullable=False, checks=pa.Check.gt(0, error="R5 low must be > 0")),
            "close": pa.Column(float, nullable=False, checks=pa.Check.gt(0, error="R5 close must be > 0")),
            "volume": pa.Column(float, nullable=False, checks=[
                pa.Check.ge(0, error="R5 volume must be >= 0"),
                pa.Check(lambda s: s % 1 == 0, error="R1 volume must be a whole number"),
            ]),
        },
        checks=[
            pa.Check(lambda d: d["high"] >= d["low"], error="R6 high < low"),
            pa.Check(lambda d: d["high"] >= d[["open", "close"]].max(axis=1), error="R6 high below open/close"),
            pa.Check(lambda d: d["low"] <= d[["open", "close"]].min(axis=1), error="R6 low above open/close"),
        ],
        unique=["symbol", "date"],
        report_duplicates="exclude_first",
        strict=False,  
    )


def validate(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return (valid_df, invalid_df). invalid_df has an `errors` column with the broken rules."""
    missing_cols = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing_cols:
        raise ValueError(f"Required columns missing from consumed data: {missing_cols}")

    df = df.reset_index(drop=True)

    
    numeric_view = df.copy()
    type_errors: dict[int, list[str]] = {}
    for col in PRICE_COLS + ["volume"]:
        coerced = pd.to_numeric(df[col], errors="coerce")
        for idx in df.index[df[col].notna() & coerced.isna()]:
            type_errors.setdefault(int(idx), []).append(f"R1 wrong data type in '{col}'")
        numeric_view[col] = coerced.astype(float)

    schema = build_schema(list(COMPANIES))
    try:
        schema.validate(numeric_view, lazy=True)  
        failures = pd.DataFrame(columns=["index", "column", "check", "failure_case"])
    except pa.errors.SchemaErrors as exc:
        failures = exc.failure_cases

    row_failures = failures[failures["index"].notna()].copy()
    if len(failures) != len(row_failures):
        raise ValueError(f"Schema-level validation failure:\n{failures[failures['index'].isna()]}")

    row_failures["index"] = row_failures["index"].astype(int)
    row_failures["detail"] = [_describe(c, k) for c, k in zip(row_failures["column"], row_failures["check"].astype(str))]
    errors: dict[int, set] = {i: set(v) for i, v in type_errors.items()}
    for idx, detail in zip(row_failures["index"], row_failures["detail"]):
        if detail.startswith("R2 missing value in") and any(detail.split("'")[1] in t for t in type_errors.get(idx, [])):
            continue
        errors.setdefault(int(idx), set()).add(detail)
    errors_by_row = pd.Series({i: " | ".join(sorted(v)) for i, v in errors.items()}, dtype=object)

    invalid_mask = df.index.isin(list(errors_by_row.index))
    invalid_df = df[invalid_mask].copy()
    invalid_df["errors"] = errors_by_row.reindex(invalid_df.index).values
    valid_df = df[~invalid_mask].copy()
    return valid_df, invalid_df


def run() -> dict:
    ensure_dirs()
    if not SETTINGS.consumed_jsonl.exists():
        raise FileNotFoundError(f"{SETTINGS.consumed_jsonl} not found - run the consumer first")
    df = pd.read_json(SETTINGS.consumed_jsonl, lines=True, dtype=False, convert_dates=False)
    if "is_test_record" not in df.columns:
        df["is_test_record"] = False
    df["is_test_record"] = df["is_test_record"].fillna(False).astype(bool)
    total = len(df)

    valid_df, invalid_df = validate(df)

    leaked_tests = valid_df["is_test_record"].sum()
    if leaked_tests:
        log.error("%d test records passed validation - the schema is too weak", leaked_tests)
    valid_df = valid_df[~valid_df["is_test_record"]]

    test_total = int(df["is_test_record"].sum())
    test_caught = int(invalid_df["is_test_record"].sum())
    real_invalid = invalid_df[~invalid_df["is_test_record"]]

    valid_df.drop(columns=["is_test_record"]).to_csv(SETTINGS.valid_csv, index=False)
    invalid_df.to_csv(SETTINGS.invalid_csv, index=False)

    result = {
        "received": total, "valid": len(valid_df), "invalid": len(invalid_df),
        "test_records": test_total, "test_records_detected": test_caught,
        "invalid_real_records": len(real_invalid),
        "invalid_handling": "rejected records are quarantined in data/invalid_records.csv and excluded from cleaning",
    }
    update_stage("validate", result)

    print("\n=== Pandera validation ===")
    print(f"received: {total} | valid: {len(valid_df)} | invalid: {len(invalid_df)} "
          f"(test records: {test_caught}/{test_total} detected, real invalid: {len(real_invalid)})")
    if len(invalid_df):
        with pd.option_context("display.max_colwidth", 120, "display.width", 200):
            print(invalid_df[["symbol", "date", "close", "volume", "is_test_record", "errors"]].head(20).to_string(index=False))
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
