"""Stage 5 - store the cleaned data in SQLite3 (idempotent).

Tables
  symbols(symbol PK, company_name, sector)
  daily_prices(PRIMARY KEY (symbol, trade_date), FK symbol -> symbols, CHECK constraints)

Idempotency: rows are written with INSERT ... ON CONFLICT(symbol, trade_date) DO UPDATE ... WHERE <values differ>.
Loading the same dataset again therefore inserts nothing and changes nothing. `run()` loads twice and compares
row counts and a SHA-256 checksum of the whole table to prove it.
"""
from __future__ import annotations

import hashlib
import logging
import math
import sqlite3
import sys

import pandas as pd

from .config import COMPANIES, SETTINGS, ensure_dirs
from .report import update_stage

log = logging.getLogger("loader")

SCHEMA = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS symbols (
    symbol        TEXT PRIMARY KEY,
    company_name  TEXT NOT NULL,
    sector        TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS daily_prices (
    symbol              TEXT    NOT NULL REFERENCES symbols(symbol),
    trade_date          TEXT    NOT NULL CHECK (date(trade_date) = trade_date),   -- ISO YYYY-MM-DD
    open                REAL    NOT NULL CHECK (open  > 0),
    high                REAL    NOT NULL CHECK (high  > 0),
    low                 REAL    NOT NULL CHECK (low   > 0),
    close               REAL    NOT NULL CHECK (close > 0),
    volume              INTEGER NOT NULL CHECK (volume > 0),
    vwap                REAL,
    price_change        REAL,
    change_percent      REAL,
    log_return          REAL,
    abs_return          REAL,
    intraday_range_pct  REAL,
    log_volume          REAL,
    rel_volume          REAL,
    log_rel_volume      REAL,
    volume_zscore       REAL,
    volatility_20d      REAL,
    direction           TEXT    NOT NULL CHECK (direction IN ('up', 'down', 'flat')),
    PRIMARY KEY (symbol, trade_date),
    CHECK (high >= low)
);

CREATE INDEX IF NOT EXISTS idx_daily_prices_date ON daily_prices(trade_date);
"""

COLUMN_MAP = {
    "symbol": "symbol", "date": "trade_date", "open": "open", "high": "high", "low": "low", "close": "close",
    "volume": "volume", "vwap": "vwap", "change": "price_change", "changePercent": "change_percent",
    "log_return": "log_return", "abs_return": "abs_return", "intraday_range_pct": "intraday_range_pct",
    "log_volume": "log_volume", "rel_volume": "rel_volume", "log_rel_volume": "log_rel_volume",
    "volume_zscore": "volume_zscore", "volatility_20d": "volatility_20d", "direction": "direction",
}
TABLE_COLS = list(COLUMN_MAP.values())
KEY_COLS = ["symbol", "trade_date"]
VALUE_COLS = [c for c in TABLE_COLS if c not in KEY_COLS]

UPSERT_SQL = f"""
INSERT INTO daily_prices ({", ".join(TABLE_COLS)})
VALUES ({", ".join("?" for _ in TABLE_COLS)})
ON CONFLICT(symbol, trade_date) DO UPDATE SET
    {", ".join(f"{c} = excluded.{c}" for c in VALUE_COLS)}
WHERE {" OR ".join(f"{c} IS NOT excluded.{c}" for c in VALUE_COLS)}
"""


def _py(v):
    if v is None:
        return None
    if hasattr(v, "item"):
        v = v.item()
    if isinstance(v, float) and math.isnan(v):
        return None
    return v


def connect() -> sqlite3.Connection:
    ensure_dirs()
    conn = sqlite3.connect(SETTINGS.db_path)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.executescript(SCHEMA)
    return conn


def table_checksum(conn: sqlite3.Connection) -> str:
    h = hashlib.sha256()
    for row in conn.execute(f"SELECT {', '.join(TABLE_COLS)} FROM daily_prices ORDER BY symbol, trade_date"):
        h.update(repr(row).encode())
    return h.hexdigest()


def load_dataframe(conn: sqlite3.Connection, df: pd.DataFrame) -> dict:
    """One load pass. Returns what it did (inserted / updated rows, counts before and after)."""
    data = df.rename(columns=COLUMN_MAP)[TABLE_COLS]
    rows = [tuple(_py(v) for v in r) for r in data.itertuples(index=False, name=None)]

    count_before = conn.execute("SELECT COUNT(*) FROM daily_prices").fetchone()[0]
    symbols_before = conn.execute("SELECT COUNT(*) FROM symbols").fetchone()[0]
    changes_before = conn.total_changes
    try:
        with conn:  
            conn.executemany(
                "INSERT OR IGNORE INTO symbols(symbol, company_name, sector) VALUES (?, ?, ?)",
                [(s, n, sec) for s, (n, sec) in COMPANIES.items()],
            )
            conn.executemany(UPSERT_SQL, rows)
    except sqlite3.Error as exc:
        raise RuntimeError(f"Database load failed (transaction rolled back): {exc}") from None
    count_after = conn.execute("SELECT COUNT(*) FROM daily_prices").fetchone()[0]
    symbols_added = conn.execute("SELECT COUNT(*) FROM symbols").fetchone()[0] - symbols_before
    inserted = count_after - count_before
    updated = conn.total_changes - changes_before - inserted - symbols_added
    return {
        "rows_processed": len(rows), "rows_before": count_before, "rows_after": count_after,
        "inserted": inserted, "updated": updated,
        "checksum": table_checksum(conn),
    }


SQL_CHECKS = {
    "total rows": "SELECT COUNT(*) AS rows FROM daily_prices",
    "rows per symbol": "SELECT symbol, COUNT(*) AS days, MIN(trade_date) AS first_day, MAX(trade_date) AS last_day "
                       "FROM daily_prices GROUP BY symbol ORDER BY symbol",
    "duplicate keys (must be empty)": "SELECT symbol, trade_date, COUNT(*) AS n FROM daily_prices "
                                      "GROUP BY symbol, trade_date HAVING n > 1",
    "NULLs in required columns (must be 0)": "SELECT COUNT(*) AS bad FROM daily_prices WHERE open IS NULL OR high IS NULL "
                                             "OR low IS NULL OR close IS NULL OR volume IS NULL",
    "top-3 volume days (join with symbols)": "SELECT s.company_name, p.trade_date, p.volume, ROUND(p.intraday_range_pct, 2) AS range_pct "
                                             "FROM daily_prices p JOIN symbols s USING(symbol) ORDER BY p.volume DESC LIMIT 3",
}


def run_sql_checks(conn: sqlite3.Connection) -> dict:
    results = {}
    print("\n=== SQL verification queries ===")
    for title, sql in SQL_CHECKS.items():
        cur = conn.execute(sql)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
        results[title] = rows
        print(f"\n-- {title}\n{sql}")
        print(pd.DataFrame(rows, columns=cols).to_string(index=False) if rows else "(no rows)")
    return results


def run() -> dict:
    ensure_dirs()
    if not SETTINGS.clean_csv.exists():
        raise FileNotFoundError(f"{SETTINGS.clean_csv} not found - run the cleaner first")
    df = pd.read_csv(SETTINGS.clean_csv)
    conn = connect()
    try:
        first = load_dataframe(conn, df)
        second = load_dataframe(conn, df)  # idempotency test: same dataset again
        checks = run_sql_checks(conn)
        duplicates = len(checks["duplicate keys (must be empty)"])
        idempotent = (
            second["inserted"] == 0 and second["updated"] == 0
            and first["rows_after"] == second["rows_after"] and first["checksum"] == second["checksum"]
        )
        result = {
            "processed_records": len(df), "stored_records": second["rows_after"],
            "first_load": first, "second_load": second, "duplicate_keys": duplicates,
            "idempotent": bool(idempotent), "db_path": str(SETTINGS.db_path),
        }
    finally:
        conn.close()

    update_stage("load", result)
    print("\n=== Idempotency test ===")
    print(f"load #1: inserted={first['inserted']} updated={first['updated']} rows={first['rows_after']}")
    print(f"load #2: inserted={second['inserted']} updated={second['updated']} rows={second['rows_after']}")
    print(f"checksum equal: {first['checksum'] == second['checksum']} -> idempotent: {idempotent}")
    print(f"processed records: {len(df)} | stored records: {second['rows_after']}")
    return result


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    try:
        res = run()
    except (FileNotFoundError, RuntimeError) as exc:
        log.error("%s", exc)
        return 1
    return 0 if res["idempotent"] else 2


if __name__ == "__main__":
    sys.exit(main())
