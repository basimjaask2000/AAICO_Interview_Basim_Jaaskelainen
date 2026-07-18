import os
import json
from typing import List, Union
from kafka import KafkaProducer, KafkaConsumer


def get_kafka_servers() -> List[str]:
    return os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").split(",")


def create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=get_kafka_servers(),
        value_serializer=lambda v: json.dumps(v).encode()
    )


def create_consumer(topics: Union[str, List[str]], group_id: str,
                    auto_offset_reset: str = "earliest") -> KafkaConsumer:
    if isinstance(topics, str):
        topics = [topics]
    return KafkaConsumer(
        *topics,
        bootstrap_servers=get_kafka_servers(),
        value_deserializer=lambda m: json.loads(m.decode()),
        group_id=group_id,
        auto_offset_reset=auto_offset_reset,
        enable_auto_commit=True,
        session_timeout_ms=30000,
        heartbeat_interval_ms=10000,
        max_poll_interval_ms=60000,
    )
