"""Servico public-export: consumidor Kafka de longa duracao que mantem os
agregados publicos em memoria e regrava o public.json a cada minuto.

- Ao iniciar, rele as 24 horas retidas no topico para se aquecer.
- Depois disso, so consome o que chega: nunca relê o topico inteiro.
- Grava public.json.tmp e troca por mv (troca atomica).
- Nada de texto, did, uri ou rkey sai no arquivo: so agregados.
"""

import argparse
import io
import json
import logging
import os
import struct
import time
from datetime import UTC, datetime

import avro.schema
import requests
from avro.io import BinaryDecoder, DatumReader
from confluent_kafka import Consumer, KafkaError, TopicPartition

from .public_aggregator import PublicAggregator
from .public_terms import load_blocklist

logging.basicConfig(
    level=logging.INFO,
    format='{"ts":"%(asctime)s","level":"%(levelname)s","msg":"%(message)s"}',
)
logger = logging.getLogger("public-export")

WARMUP_WINDOW_S = 24 * 60 * 60
POLL_IDLE_S = 5.0


class SchemaCache:
    def __init__(self, registry_url: str):
        self.registry_url = registry_url.rstrip("/")
        self._schemas: dict[int, avro.schema.Schema] = {}

    def get(self, schema_id: int) -> avro.schema.Schema:
        if schema_id not in self._schemas:
            response = requests.get(
                f"{self.registry_url}/schemas/ids/{schema_id}", timeout=10
            )
            response.raise_for_status()
            self._schemas[schema_id] = avro.schema.parse(response.json()["schema"])
        return self._schemas[schema_id]

    def decode(self, value: bytes, schemas: "SchemaCache") -> dict | None:
        if not value or len(value) < 5:
            return None
        magic, schema_id = struct.unpack(">bI", value[:5])
        if magic != 0:
            return None
        schema = schemas.get(schema_id)
        return DatumReader(schema).read(BinaryDecoder(io.BytesIO(value[5:])))


def _write_atomic(payload: dict, output: str) -> None:
    directory = os.path.dirname(output) or "."
    os.makedirs(directory, exist_ok=True)
    tmp = f"{output}.tmp"
    with open(tmp, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, separators=(",", ":"))
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(tmp, output)


def _topic_watermarks(consumer: Consumer, topic: str) -> dict[int, tuple[int, int]]:
    """(low, high) por particao, para o aquecimento e o lag."""
    metadata = consumer.list_topics(topic, timeout=10)
    partitions = metadata.topics[topic].partitions
    marks: dict[int, tuple[int, int]] = {}
    for partition in partitions:
        low, high = consumer.get_watermark_offsets(
            TopicPartition(topic, partition), timeout=10, cached=False
        )
        marks[partition] = (low, high)
    return marks


def warm_up(
    consumer: Consumer, topic: str, aggregator: PublicAggregator, schemas: SchemaCache
) -> int:
    """Le o topico do inicio ate o fim atual, uma vez, para encher a memoria."""
    marks = _topic_watermarks(consumer, topic)
    consumer.assign([TopicPartition(topic, p, low) for p, (low, _) in marks.items()])
    targets = {p: high for p, (_, high) in marks.items()}
    positions = {p: low for p, (low, _) in marks.items()}
    seen = 0
    idle_polls = 0
    while any(positions[p] < targets[p] for p in targets):
        msg = consumer.poll(1.0)
        if msg is None:
            idle_polls += 1
            if idle_polls > 30:
                break
            continue
        idle_polls = 0
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                positions[msg.partition()] = targets[msg.partition()]
                continue
            continue
        positions[msg.partition()] = msg.offset() + 1
        record = schemas.decode(msg.value(), schemas)
        if record and aggregator.add(record):
            seen += 1
    logger.info("warmup_done", extra={"posts": seen})
    return seen


def main() -> None:
    parser = argparse.ArgumentParser(description="Agregados publicos do painel")
    parser.add_argument(
        "--bootstrap",
        default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
    )
    parser.add_argument(
        "--registry",
        default=os.environ.get("SCHEMA_REGISTRY_URL", "http://schema-registry:8081"),
    )
    parser.add_argument(
        "--topic", default=os.environ.get("KAFKA_TOPIC", "bluesky.posts.pt.v1")
    )
    parser.add_argument(
        "--output", default=os.environ.get("PUBLIC_OUTPUT", "/public/public.json")
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=float(os.environ.get("PUBLIC_INTERVAL_S", "60")),
    )
    parser.add_argument(
        "--min-authors",
        type=int,
        default=int(os.environ.get("PUBLIC_MIN_AUTHORS", "10")),
    )
    parser.add_argument(
        "--blocklist",
        default=os.environ.get("PUBLIC_BLOCKLIST", ""),
    )
    args = parser.parse_args()

    blocklist = load_blocklist(args.blocklist) if args.blocklist else load_blocklist()
    aggregator = PublicAggregator(min_authors=args.min_authors, blocklist=blocklist)
    schemas = SchemaCache(args.registry)

    consumer = Consumer(
        {
            "bootstrap.servers": args.bootstrap,
            "group.id": f"public-export-{os.environ.get('HOSTNAME', 'local')}",
            "enable.auto.commit": False,
            "auto.offset.reset": "latest",
            "session.timeout.ms": 30000,
            "max.poll.interval.ms": 600000,
        }
    )

    logger.info(
        "public_export_started",
        extra={"output": args.output, "min_authors": args.min_authors},
    )
    try:
        warm_up(consumer, args.topic, aggregator, schemas)
        # depois do aquecimento, so o que chegar de agora em diante
        marks = _topic_watermarks(consumer, args.topic)
        consumer.assign(
            [TopicPartition(args.topic, p, high) for p, (_, high) in marks.items()]
        )
    except Exception as exc:  # sem aquecimento o servico ainda serve dados novos
        logger.error("warmup_failed", extra={"error": str(exc)})

    last_write = 0.0
    while True:
        try:
            msg = consumer.poll(POLL_IDLE_S)
            if msg is not None and not msg.error():
                record = schemas.decode(msg.value(), schemas)
                if record:
                    aggregator.add(record)
            now = time.monotonic()
            if now - last_write >= args.interval:
                _write_atomic(aggregator.snapshot(datetime.now(UTC)), args.output)
                last_write = now
                logger.info("public_written", extra={"output": args.output})
        except Exception as exc:
            logger.error("public_export_cycle_failed", extra={"error": str(exc)})
            time.sleep(5)


if __name__ == "__main__":
    main()
