"""Stage 2a - Kafka Producer.

Reads data/raw_data.csv and publishes every record as a JSON message to the Kafka topic.
Message key   = ticker symbol (all records of a ticker go to one partition, in order)
Message value = JSON object (UTF-8)
Header        = run_id, so the consumer can ignore messages left in the topic by previous runs.

To demonstrate Pandera validation, three deliberately invalid TEST records are appended to the stream.
They are marked with "is_test_record": true and are excluded from the final dataset by the validator.
"""
from __future__ import annotations

import argparse
import json
import logging
import math
import sys
import uuid
from datetime import datetime

import pandas as pd
from confluent_kafka import KafkaException, Producer

from .config import SETTINGS, ensure_dirs
from .kafka_utils import BrokerUnavailable, ensure_broker_and_topic
from .report import update_stage

log = logging.getLogger("producer")


def build_test_records() -> list[dict]:
    """Three clearly identified invalid records (each breaks several rules)."""
    base = {"change": 0.0, "changePercent": 0.0, "vwap": 100.0, "is_test_record": True}
    return [
        # 1) negative close + negative volume
        {**base, "symbol": "AAPL", "date": "2099-01-04", "open": 100.0, "high": 105.0, "low": 95.0, "close": -50.0, "volume": -1000},
        # 2) high < low  (impossible price range)
        {**base, "symbol": "MSFT", "date": "2099-01-05", "open": 200.0, "high": 190.0, "low": 210.0, "close": 200.0, "volume": 5_000_000},
        # 3) unknown ticker + malformed date + missing close
        {**base, "symbol": "FAKE", "date": "not-a-date", "open": 10.0, "high": 12.0, "low": 9.0, "close": None, "volume": 1000},
    ]


def _clean_value(v):
    """JSON has no NaN: turn NaN/NaT into None, numpy scalars into native Python."""
    if v is None:
        return None
    if isinstance(v, float) and math.isnan(v):
        return None
    if hasattr(v, "item"):
        v = v.item()
        if isinstance(v, float) and math.isnan(v):
            return None
    return v


def load_records(include_test_records: bool = True) -> list[dict]:
    if not SETTINGS.raw_csv.exists():
        raise FileNotFoundError(f"{SETTINGS.raw_csv} not found - run the collection stage first (python -m src.api_client)")
    df = pd.read_csv(SETTINGS.raw_csv)
    records = [{k: _clean_value(v) for k, v in row.items()} for row in df.to_dict(orient="records")]
    for rec in records:
        rec["is_test_record"] = False
    if include_test_records:
        records.extend(build_test_records())
    return records


def run(include_test_records: bool = True) -> dict:
    ensure_dirs()
    info = ensure_broker_and_topic()
    records = load_records(include_test_records)
    run_id = uuid.uuid4().hex[:12]

    stats = {"delivered": 0, "failed": 0, "errors": []}

    def on_delivery(err, msg):
        if err is not None:
            stats["failed"] += 1
            if len(stats["errors"]) < 5:
                stats["errors"].append(str(err))
        else:
            stats["delivered"] += 1

    producer = Producer({
        "bootstrap.servers": SETTINGS.kafka_bootstrap,
        "acks": "all",
        "enable.idempotence": True,  
        "linger.ms": 20,
        "compression.type": "gzip",
        "error_cb": lambda err: log.error("Kafka error: %s", err),
    })

    serialization_errors = 0
    for rec in records:
        try:
            value = json.dumps(rec, ensure_ascii=False).encode("utf-8")  
        except (TypeError, ValueError) as exc:
            serialization_errors += 1
            log.error("Cannot serialise %s: %s", rec.get("symbol"), exc)
            continue
        while True:
            try:
                producer.produce(
                    SETTINGS.kafka_topic, key=str(rec["symbol"]).encode(), value=value,
                    headers=[("run_id", run_id.encode())], on_delivery=on_delivery,
                )
                break
            except BufferError:  
                producer.poll(0.5)
            except KafkaException as exc:
                stats["failed"] += 1
                log.error("Produce failed: %s", exc)
                break
        producer.poll(0)

    remaining = producer.flush(60)
    if remaining:
        log.error("%d messages were not delivered before the flush timeout", remaining)
        stats["failed"] += remaining

    result = {
        "run_id": run_id, "topic": SETTINGS.kafka_topic, "attempted": len(records),
        "published": stats["delivered"], "failed": stats["failed"],
        "serialization_errors": serialization_errors, "test_records_sent": 3 if include_test_records else 0,
        "delivery_errors_sample": stats["errors"], "broker": info,
        "finished_at": datetime.now().isoformat(timespec="seconds"),
    }
    SETTINGS.manifest_json.write_text(json.dumps(result, indent=2), encoding="utf-8")
    update_stage("produce", result)
    log.info("Published %d / %d messages (failed: %d) run_id=%s", stats["delivered"], len(records), stats["failed"], run_id)
    return result


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    parser = argparse.ArgumentParser(description="Publish raw API records to Kafka")
    parser.add_argument("--no-test-records", action="store_true", help="do not append the 3 invalid test records")
    args = parser.parse_args()
    try:
        res = run(include_test_records=not args.no_test_records)
    except (BrokerUnavailable, FileNotFoundError) as exc:
        log.error("%s", exc)
        return 1
    print(f"\nPublished messages: {res['published']}  (failed: {res['failed']})")
    return 0 if res["failed"] == 0 else 2


if __name__ == "__main__":
    sys.exit(main())
