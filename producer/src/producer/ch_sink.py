"""Servico ch-sink: le as janelas do Kafka e grava no ClickHouse.

Por que nao um sink JDBC no Flink: o conector JDBC do Flink 3.4 nao tem factory
para ClickHouse, e forcar a URL de outro banco faz o driver errado ser carregado.
Um consumidor Python resolve com o protocolo HTTP nativo do ClickHouse, e o
Flink continua sendo quem calcula a janela.

- Consome bluesky.trending.v1 e bluesky.volume.v1 (upsert-kafka, JSON).
- Grava em bluesky.trending_terms e bluesky.volume_minutes.
- Deduplicacao pelo ReplacingMergeTree: reenviar a mesma janela substitui a linha.
- Commit de offset so depois do INSERT dar certo, para nao perder janela.
"""

import argparse
import json
import logging
import os
import time
from datetime import datetime
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from confluent_kafka import Consumer

logging.basicConfig(
    level=logging.INFO,
    format='{"ts":"%(asctime)s","level":"%(levelname)s","msg":"%(message)s"}',
)
logger = logging.getLogger("ch-sink")

TRENDING_TOPIC = "bluesky.trending.v1"
VOLUME_TOPIC = "bluesky.volume.v1"
BATCH_ROWS = 500
FLUSH_S = 10.0


def parse_flink_time(value) -> str | None:
    """Timestamp do JSON do Flink como 'YYYY-MM-DD HH:MM:SS' (formato do CH)."""
    if not isinstance(value, str) or not value:
        return None
    text = value.replace("T", " ").removesuffix("Z").split(".")[0]
    try:
        datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None
    return text


def insert(
    clickhouse_url: str, table: str, columns: list[str], rows: list[list]
) -> None:
    """INSERT em lote via HTTP, formato JSONEachRow."""
    if not rows:
        return
    payload = "\n".join(
        json.dumps(dict(zip(columns, row, strict=True)), ensure_ascii=False)
        for row in rows
    )
    query = f"INSERT INTO {table} ({', '.join(columns)}) FORMAT JSONEachRow"
    url = f"{clickhouse_url.rstrip('/')}/?{urlencode({'query': query})}"
    request = Request(
        url,
        data=payload.encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urlopen(request, timeout=30) as response:  # noqa: S310 - url interna
        response.read()


def decode(raw: bytes | None) -> dict | None:
    if not raw:
        return None
    try:
        parsed = json.loads(raw)
    except (ValueError, UnicodeDecodeError):
        return None
    return parsed if isinstance(parsed, dict) else None


def main() -> None:
    parser = argparse.ArgumentParser(description="Kafka -> ClickHouse")
    parser.add_argument(
        "--bootstrap",
        default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
    )
    parser.add_argument(
        "--clickhouse",
        default=os.environ.get("CLICKHOUSE_URL", "http://clickhouse:8123"),
    )
    parser.add_argument(
        "--min-authors", type=int, default=int(os.environ.get("CH_MIN_AUTHORS", "1"))
    )
    args = parser.parse_args()

    consumer = Consumer(
        {
            "bootstrap.servers": args.bootstrap,
            "group.id": "ch-sink",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([TRENDING_TOPIC, VOLUME_TOPIC])
    logger.info("ch_sink_started", extra={"clickhouse": args.clickhouse})

    trending: dict[tuple, list] = {}
    volume: dict[tuple, list] = {}
    last_flush = time.monotonic()

    def flush() -> None:
        nonlocal last_flush
        if trending:
            rows = list(trending.values())
            insert(
                args.clickhouse,
                "bluesky.trending_terms",
                ["window_start", "window_end", "term", "kind", "authors", "posts"],
                rows,
            )
            logger.info("trending_flushed", extra={"rows": len(rows)})
            trending.clear()
        if volume:
            rows = list(volume.values())
            insert(
                args.clickhouse,
                "bluesky.volume_minutes",
                ["window_start", "window_end", "posts"],
                rows,
            )
            logger.info("volume_flushed", extra={"rows": len(rows)})
            volume.clear()
        consumer.commit(asynchronous=False)
        last_flush = time.monotonic()

    while True:
        try:
            for msg in consumer.consume(num_messages=500, timeout=1.0):
                if msg.error():
                    continue
                value = decode(msg.value())
                if not value:
                    continue
                start = parse_flink_time(value.get("window_start"))
                end = parse_flink_time(value.get("window_end"))
                if start is None or end is None:
                    continue
                if msg.topic() == TRENDING_TOPIC:
                    term = value.get("term")
                    kind = value.get("kind")
                    authors = int(value.get("authors") or 0)
                    if not term or kind not in ("hashtag", "term"):
                        continue
                    if authors < args.min_authors:
                        continue
                    trending[(start, kind, term)] = [
                        start,
                        end,
                        term,
                        kind,
                        authors,
                        int(value.get("posts") or 0),
                    ]
                else:
                    volume[(start,)] = [start, end, int(value.get("posts") or 0)]

            if len(trending) + len(volume) >= BATCH_ROWS or (
                time.monotonic() - last_flush >= FLUSH_S and (trending or volume)
            ):
                flush()
        except Exception as exc:  # banco reiniciando, tabela ainda nao criada
            logger.error("ch_sink_cycle_failed", extra={"error": str(exc)})
            time.sleep(5)


if __name__ == "__main__":
    main()
