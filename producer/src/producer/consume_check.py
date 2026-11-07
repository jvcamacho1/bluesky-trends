import io
import json
import struct
import sys
import time
import uuid

import avro.schema
import requests
from avro.io import BinaryDecoder, DatumReader
from confluent_kafka import Consumer


def schema_by_id(registry_url: str, schema_id: int) -> avro.schema.Schema:
    response = requests.get(
        f"{registry_url.rstrip('/')}/schemas/ids/{schema_id}", timeout=10
    )
    response.raise_for_status()
    return avro.schema.parse(response.json()["schema"])


def consume(
    bootstrap_servers: str,
    registry_url: str,
    topic: str,
    max_messages: int,
    timeout_s: float = 60.0,
    raw: bool = False,
) -> list[dict]:
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": f"consume-check-{topic}-{uuid.uuid4()}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([topic])
    schemas: dict[int, avro.schema.Schema] = {}
    messages: list[dict] = []
    end = time.monotonic() + timeout_s
    while len(messages) < max_messages and time.monotonic() < end:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            continue
        if raw:
            messages.append(
                {
                    "_value": msg.value().decode("utf-8", errors="replace"),
                    "_topic": msg.topic(),
                    "_headers": {
                        k: v.decode("utf-8", errors="replace")
                        for k, v in (msg.headers() or [])
                    },
                }
            )
            continue
        magic, schema_id = struct.unpack(">bI", msg.value()[:5])
        if magic != 0:
            continue
        if schema_id not in schemas:
            schemas[schema_id] = schema_by_id(registry_url, schema_id)
        record = DatumReader(schemas[schema_id]).read(
            BinaryDecoder(io.BytesIO(msg.value()[5:]))
        )
        record["_key"] = msg.key().decode() if msg.key() else None
        record["_topic"] = msg.topic()
        messages.append(record)
    consumer.close()
    return messages


def main() -> None:
    bootstrap = sys.argv[1] if len(sys.argv) > 1 else "localhost:9092"
    registry = sys.argv[2] if len(sys.argv) > 2 else "http://localhost:8081"
    topic = sys.argv[3] if len(sys.argv) > 3 else "bluesky.posts.pt.v1"
    max_messages = int(sys.argv[4]) if len(sys.argv) > 4 else 10
    timeout = float(sys.argv[5]) if len(sys.argv) > 5 else 60.0
    raw = len(sys.argv) > 6 and sys.argv[6] == "raw"
    for record in consume(bootstrap, registry, topic, max_messages, timeout, raw):
        print(json.dumps(record, ensure_ascii=False))


if __name__ == "__main__":
    main()
