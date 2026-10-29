"""In-memory topic log and an optional Redpanda (Kafka API) bus.

Both paths publish the same JSON contracts. Replay and CI use the in-memory
bus so the producer, schema check, and consumer are shared without Docker.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

import httpx

from aus_hazard.contracts import HOTSPOT_SUBJECT, QUAKE_SUBJECT, load_schema


@dataclass
class Message:
    topic: str
    key: str
    value: dict[str, Any]


@dataclass
class InMemorySchemaRegistry:
    """Stand-in for the Redpanda schema registry used when Kafka is down."""

    subjects: dict[str, list[dict[str, Any]]] = field(default_factory=dict)

    def register(self, subject: str, schema: dict[str, Any]) -> int:
        versions = self.subjects.setdefault(subject, [])
        versions.append(schema)
        return len(versions)

    def latest(self, subject: str) -> dict[str, Any] | None:
        versions = self.subjects.get(subject) or []
        return versions[-1] if versions else None


def register_local_schemas(registry: InMemorySchemaRegistry) -> None:
    registry.register(QUAKE_SUBJECT, load_schema("quake_event.v1.json"))
    registry.register(QUAKE_SUBJECT, load_schema("quake_event.v2.json"))
    registry.register(HOTSPOT_SUBJECT, load_schema("hotspot_event.v1.json"))


class InMemoryBus:
    def __init__(self) -> None:
        self.topics: dict[str, list[Message]] = {}
        self.registry = InMemorySchemaRegistry()
        register_local_schemas(self.registry)

    def publish(self, topic: str, key: str, value: dict[str, Any]) -> None:
        self.topics.setdefault(topic, []).append(Message(topic=topic, key=key, value=value))

    def read(self, topic: str) -> list[Message]:
        return list(self.topics.get(topic, []))


def register_redpanda_schemas(registry_url: str) -> None:
    """Register JSON schemas with Redpanda's schema registry (no auth)."""
    subjects = {
        QUAKE_SUBJECT: ["quake_event.v1.json", "quake_event.v2.json"],
        HOTSPOT_SUBJECT: ["hotspot_event.v1.json"],
    }
    with httpx.Client(timeout=30) as client:
        for subject, names in subjects.items():
            for name in names:
                schema = load_schema(name)
                response = client.post(
                    f"{registry_url.rstrip('/')}/subjects/{subject}/versions",
                    headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
                    json={"schemaType": "JSON", "schema": json.dumps(schema)},
                )
                response.raise_for_status()


async def publish_kafka(brokers: str, messages: list[Message]) -> None:
    from aiokafka import AIOKafkaProducer

    producer = AIOKafkaProducer(
        bootstrap_servers=brokers,
        value_serializer=lambda value: json.dumps(value).encode(),
        key_serializer=lambda key: key.encode(),
    )
    await producer.start()
    try:
        for message in messages:
            await producer.send_and_wait(message.topic, value=message.value, key=message.key)
    finally:
        await producer.stop()
