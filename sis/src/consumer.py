"""Stage 2b - Kafka Consumer.

Reads the messages published by the producer (same run_id), deserialises the JSON and stores them in
data/consumed_data.jsonl.

Termination conditions (so the pipeline always finishes):
  1. the number of messages announced in data/kafka_manifest.json has been consumed, OR
  2. no new message arrived for CONSUMER_IDLE_TIMEOUT seconds, OR
  3. CONSUMER_MAX_RUNTIME seconds have passed.
"""
from __future__ import annotations

import json
import logging
import sys
import time
import uuid
from datetime import datetime

from confluent_kafka import Consumer, KafkaError, KafkaException

from .config import SETTINGS, ensure_dirs
from .kafka_utils import BrokerUnavailable, ensure_broker_and_topic
from .report import update_stage

log = logging.getLogger("consumer")


def _load_manifest() -> dict:
    if SETTINGS.manifest_json.exists():
        return json.loads(SETTINGS.manifest_json.read_text(encoding="utf-8"))
    return {}


def _header(msg, name: str):
    for k, v in msg.headers() or []:
        if k == name:
            return v.decode() if isinstance(v, bytes) else v
    return None


def run() -> dict:
    ensure_dirs()
    info = ensure_broker_and_topic()
    manifest = _load_manifest()
    run_id = manifest.get("run_id")
    expected = manifest.get("published")  
    if run_id is None:
        log.warning("No manifest found - consuming everything in the topic until the idle timeout")

    consumer = Consumer({
        "bootstrap.servers": SETTINGS.kafka_bootstrap,
        "group.id": f"stock-consumer-{uuid.uuid4().hex[:8]}", 
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "enable.partition.eof": False,
    })
    consumer.subscribe([SETTINGS.kafka_topic])

    consumed, skipped_other_runs, malformed, kafka_errors = [], 0, 0, 0
    started = last_message = time.monotonic()
    stop_reason = None

    try:
        with open(SETTINGS.malformed_jsonl, "w", encoding="utf-8") as bad_file:
            while True:
                now = time.monotonic()
                if expected is not None and len(consumed) + malformed >= expected:
                    stop_reason = "all expected messages consumed"
                    break
                if now - last_message > SETTINGS.consumer_idle_timeout:
                    stop_reason = f"idle for {SETTINGS.consumer_idle_timeout}s"
                    break
                if now - started > SETTINGS.consumer_max_runtime:
                    stop_reason = f"max runtime {SETTINGS.consumer_max_runtime}s reached"
                    break

                msg = consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    kafka_errors += 1
                    log.error("Consumer error: %s", msg.error())
                    if msg.error().fatal():
                        stop_reason = f"fatal Kafka error: {msg.error()}"
                        break
                    continue

                last_message = time.monotonic()
                if run_id is not None and _header(msg, "run_id") != run_id:
                    skipped_other_runs += 1  
                    continue
                try:
                    record = json.loads(msg.value().decode("utf-8"))  
                    if not isinstance(record, dict):
                        raise ValueError("message is not a JSON object")
                except (ValueError, UnicodeDecodeError) as exc:
                    malformed += 1
                    bad_file.write(json.dumps({"offset": msg.offset(), "partition": msg.partition(), "error": str(exc)}) + "\n")
                    continue
                consumed.append(record)

        consumer.commit(asynchronous=False) if consumed else None
    except KafkaException as exc:
        raise BrokerUnavailable(f"Kafka consumer failed: {exc}") from None
    finally:
        consumer.close()

    with open(SETTINGS.consumed_jsonl, "w", encoding="utf-8") as f:
        for rec in consumed:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")

    result = {
        "run_id": run_id, "topic": SETTINGS.kafka_topic, "expected": expected, "consumed": len(consumed),
        "malformed": malformed, "skipped_from_other_runs": skipped_other_runs, "kafka_errors": kafka_errors,
        "stop_reason": stop_reason, "broker": info, "finished_at": datetime.now().isoformat(timespec="seconds"),
    }
    update_stage("consume", result)
    log.info("Consumed %d messages (expected %s). Stop reason: %s", len(consumed), expected, stop_reason)
    return result


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    try:
        res = run()
    except BrokerUnavailable as exc:
        log.error("%s", exc)
        return 1
    print(f"\nConsumed messages: {res['consumed']}  (expected: {res['expected']}, stop: {res['stop_reason']})")
    return 0 if res["consumed"] > 0 else 2


if __name__ == "__main__":
    sys.exit(main())
