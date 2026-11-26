from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

from dotenv import load_dotenv

ROOT_DIR = Path(__file__).resolve().parent.parent
load_dotenv(ROOT_DIR / ".env")

DATA_DIR = ROOT_DIR / "data"
FIGURES_DIR = ROOT_DIR / "figures"
DOCS_DIR = ROOT_DIR / "docs"

COMPANIES = {
    "AAPL": ("Apple Inc.", "Technology"),
    "MSFT": ("Microsoft Corporation", "Technology"),
    "NVDA": ("NVIDIA Corporation", "Technology"),
    "AMZN": ("Amazon.com, Inc.", "Consumer Discretionary"),
    "GOOGL": ("Alphabet Inc.", "Communication Services"),
}


def _default_start() -> str:
    return (date.today() - timedelta(days=3 * 365)).isoformat()


@dataclass(frozen=True)
class Settings:
    fmp_api_key: str = os.getenv("FMP_API_KEY", "")
    fmp_base_url: str = os.getenv("FMP_BASE_URL", "https://financialmodelingprep.com/stable")
    symbols: tuple = tuple(s.strip().upper() for s in os.getenv("SYMBOLS", ",".join(COMPANIES)).split(",") if s.strip())
    start_date: str = os.getenv("START_DATE", _default_start())
    end_date: str = os.getenv("END_DATE", date.today().isoformat())
    request_timeout: int = int(os.getenv("REQUEST_TIMEOUT", "30"))
    request_retries: int = int(os.getenv("REQUEST_RETRIES", "4"))

    kafka_bootstrap: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    kafka_topic: str = os.getenv("KAFKA_TOPIC", "stock_prices")
    kafka_partitions: int = int(os.getenv("KAFKA_PARTITIONS", "3"))
    consumer_idle_timeout: int = int(os.getenv("CONSUMER_IDLE_TIMEOUT", "30"))  
    consumer_max_runtime: int = int(os.getenv("CONSUMER_MAX_RUNTIME", "300")) 

    raw_csv: Path = DATA_DIR / "raw_data.csv"
    manifest_json: Path = DATA_DIR / "kafka_manifest.json"
    consumed_jsonl: Path = DATA_DIR / "consumed_data.jsonl"
    malformed_jsonl: Path = DATA_DIR / "malformed_messages.jsonl"
    valid_csv: Path = DATA_DIR / "valid_data.csv"
    invalid_csv: Path = DATA_DIR / "invalid_records.csv"
    clean_csv: Path = DATA_DIR / "clean_data.csv"
    db_path: Path = DATA_DIR / "output.db"
    report_json: Path = DATA_DIR / "pipeline_report.json"


SETTINGS = Settings()


def ensure_dirs() -> None:
    for d in (DATA_DIR, FIGURES_DIR, DOCS_DIR):
        d.mkdir(parents=True, exist_ok=True)
