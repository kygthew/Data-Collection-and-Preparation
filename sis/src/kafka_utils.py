from __future__ import annotations

import logging

from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

from .config import SETTINGS

log = logging.getLogger("kafka_utils")


class BrokerUnavailable(RuntimeError):
    pass


def admin_client() -> AdminClient:
    return AdminClient({"bootstrap.servers": SETTINGS.kafka_bootstrap, "socket.timeout.ms": 10000})


def ensure_broker_and_topic() -> dict:
    """Fail fast if the broker is unreachable; create the topic if it does not exist yet."""
    admin = admin_client()
    try:
        metadata = admin.list_topics(timeout=10)
    except KafkaException as exc:
        raise BrokerUnavailable(
            f"Cannot reach Kafka broker at {SETTINGS.kafka_bootstrap}: {exc}. "
            "Is it running? Start it with: docker compose up -d"
        ) from None

    if SETTINGS.kafka_topic not in metadata.topics:
        log.info("Topic %r not found - creating it", SETTINGS.kafka_topic)
        futures = admin.create_topics([NewTopic(SETTINGS.kafka_topic, SETTINGS.kafka_partitions, 1)])
        for topic, fut in futures.items():
            try:
                fut.result()
            except KafkaException as exc:
                if "TOPIC_ALREADY_EXISTS" not in str(exc):
                    raise BrokerUnavailable(f"Could not create topic {topic}: {exc}") from None
        metadata = admin.list_topics(timeout=10)

    topic_md = metadata.topics[SETTINGS.kafka_topic]
    return {
        "bootstrap_servers": SETTINGS.kafka_bootstrap,
        "topic": SETTINGS.kafka_topic,
        "partitions": len(topic_md.partitions),
        "broker_count": len(metadata.brokers),
    }
