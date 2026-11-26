"""Stage 1 - collect daily OHLCV data from the Financial Modeling Prep (FMP) API.

Endpoint : GET {FMP_BASE_URL}/historical-price-eod/full
Params   : symbol, from (YYYY-MM-DD), to (YYYY-MM-DD), apikey
Response : JSON list of {symbol, date, open, high, low, close, volume, change, changePercent, vwap}

The untouched response data is stored in data/raw_data.csv *before* any cleaning.
"""
from __future__ import annotations

import logging
import sys

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .config import SETTINGS, ensure_dirs
from .report import update_stage

log = logging.getLogger("api_client")
ENDPOINT = "historical-price-eod/full"


class ApiError(RuntimeError):
    pass


def _session() -> requests.Session:
    retry = Retry(
        total=SETTINGS.request_retries,
        backoff_factor=1.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
    )
    s = requests.Session()
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.mount("http://", HTTPAdapter(max_retries=retry))
    return s


def _mask(text: str) -> str:
    """Never leak the API key into logs / exception messages."""
    key = SETTINGS.fmp_api_key
    return text.replace(key, "***") if key else text


def fetch_symbol(session: requests.Session, symbol: str) -> list[dict]:
    params = {"symbol": symbol, "from": SETTINGS.start_date, "to": SETTINGS.end_date, "apikey": SETTINGS.fmp_api_key}
    url = f"{SETTINGS.fmp_base_url}/{ENDPOINT}"
    try:
        resp = session.get(url, params=params, timeout=SETTINGS.request_timeout)
    except requests.RequestException as exc:
        raise ApiError(f"{symbol}: request failed: {_mask(str(exc))}") from None

    if resp.status_code in (401, 402, 403):
        raise ApiError(f"{symbol}: HTTP {resp.status_code} - invalid API key or endpoint not in your FMP plan")
    if resp.status_code != 200:
        raise ApiError(f"{symbol}: HTTP {resp.status_code}: {_mask(resp.text[:200])}")

    try:
        payload = resp.json()
    except ValueError:
        raise ApiError(f"{symbol}: response is not valid JSON") from None

    if isinstance(payload, dict):
        raise ApiError(f"{symbol}: API error: {_mask(str(payload.get('Error Message') or payload)[:200])}")
    if not isinstance(payload, list) or not payload:
        raise ApiError(f"{symbol}: empty response for {SETTINGS.start_date}..{SETTINGS.end_date}")
    return payload


def collect() -> pd.DataFrame:
    ensure_dirs()
    if not SETTINGS.fmp_api_key:
        raise ApiError("FMP_API_KEY is not set. Copy .env.example to .env and put your key there.")

    session = _session()
    rows: list[dict] = []
    per_symbol: dict[str, int] = {}
    failed: dict[str, str] = {}
    for symbol in SETTINGS.symbols:
        try:
            records = fetch_symbol(session, symbol)
        except ApiError as exc:
            log.error("%s", exc)
            failed[symbol] = str(exc)
            continue
        for rec in records:
            rec.setdefault("symbol", symbol)  
        rows.extend(records)
        per_symbol[symbol] = len(records)
        log.info("%s: %d records", symbol, len(records))

    if not rows:
        raise ApiError("No data collected for any symbol.")

    df = pd.DataFrame(rows)
    df.to_csv(SETTINGS.raw_csv, index=False)  
    log.info("Saved %d raw records -> %s", len(df), SETTINGS.raw_csv)
    update_stage("collect", {
        "total_records": len(df), "per_symbol": per_symbol, "failed_symbols": failed,
        "date_from": SETTINGS.start_date, "date_to": SETTINGS.end_date,
    })
    if failed:
        log.warning("Some symbols failed: %s", list(failed))
    return df


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    try:
        df = collect()
    except ApiError as exc:
        log.error("%s", exc)
        return 1
    print(f"\nCollected records: {len(df)}")
    print(df.head())
    return 0


if __name__ == "__main__":
    sys.exit(main())
