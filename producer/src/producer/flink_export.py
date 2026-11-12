"""Servico flink-export: le as janelas que o Flink grava no Kafka e regrava o
flink.json a cada 30 segundos.

- Topicos de entrada: bluesky.trending.v1 e bluesky.volume.v1 (upsert-kafka,
  chave e valor em JSON). Nenhum dos dois tem texto de post nem did.
- Ao iniciar, nao rele as 24 horas dos topicos: pula direto para os ultimos
  minutos com offsets_for_times, que e tudo o que a pagina mostra.
- Grava flink.json.tmp e troca por os.replace (troca atomica).
"""

import argparse
import json
import logging
import os
import time
from datetime import UTC, datetime, timedelta

from confluent_kafka import OFFSET_END, Consumer, TopicPartition

from .flink_view import SPARK_WINDOWS, VOLUME_MINUTES, FlinkView
from .public_export import _write_atomic
from .public_terms import load_blocklist

logging.basicConfig(
    level=logging.INFO,
    format='{"ts":"%(asctime)s","level":"%(levelname)s","msg":"%(message)s"}',
)
logger = logging.getLogger("flink-export")

TRENDING_TOPIC = "bluesky.trending.v1"
VOLUME_TOPIC = "bluesky.volume.v1"
# folga sobre o que a pagina mostra: watermark de 2 min e atraso de emissao
LOOKBACK = {
    TRENDING_TOPIC: timedelta(minutes=SPARK_WINDOWS + 8),
    VOLUME_TOPIC: timedelta(minutes=VOLUME_MINUTES + 8),
}


def decode_json(raw: bytes | None) -> dict | None:
    if not raw:
        return None
    try:
        decoded = json.loads(raw)
    except (ValueError, UnicodeDecodeError):
        return None
    return decoded if isinstance(decoded, dict) else None


def apply_message(view: FlinkView, topic: str, key, value) -> bool:
    """Roteia uma mensagem do Kafka para o lado certo da visao."""
    parsed_key = decode_json(key)
    parsed_value = decode_json(value)
    if value and parsed_value is None:
        return False  # valor ilegivel nao e tombstone
    if topic == TRENDING_TOPIC:
        return view.add_trending(parsed_key, parsed_value)
    if topic == VOLUME_TOPIC:
        return view.add_volume(parsed_key, parsed_value)
    return False


def start_positions(consumer: Consumer, now: datetime) -> list[TopicPartition]:
    """Offset de cada particao no instante now - LOOKBACK do topico."""
    wanted = []
    for topic, lookback in LOOKBACK.items():
        metadata = consumer.list_topics(topic, timeout=10)
        since_ms = int((now - lookback).timestamp() * 1000)
        for partition in metadata.topics[topic].partitions:
            wanted.append(TopicPartition(topic, partition, since_ms))
    found = consumer.offsets_for_times(wanted, timeout=10)
    # particao sem mensagem depois do instante devolve -1: comeca do fim
    return [
        TopicPartition(
            tp.topic, tp.partition, tp.offset if tp.offset >= 0 else OFFSET_END
        )
        for tp in found
    ]


def main() -> None:
    parser = argparse.ArgumentParser(description="flink.json da pagina /flink/")
    parser.add_argument(
        "--bootstrap",
        default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
    )
    parser.add_argument(
        "--output", default=os.environ.get("FLINK_OUTPUT", "/public/flink.json")
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=float(os.environ.get("FLINK_INTERVAL_S", "30")),
    )
    parser.add_argument(
        "--min-authors",
        type=int,
        default=int(os.environ.get("FLINK_MIN_AUTHORS", "10")),
    )
    args = parser.parse_args()

    view = FlinkView(min_authors=args.min_authors, blocklist=load_blocklist())
    consumer = Consumer(
        {
            "bootstrap.servers": args.bootstrap,
            "group.id": f"flink-export-{os.environ.get('HOSTNAME', 'local')}",
            "enable.auto.commit": False,
            "auto.offset.reset": "latest",
        }
    )
    logger.info(
        "flink_export_started",
        extra={"output": args.output, "min_authors": args.min_authors},
    )

    assigned = False
    last_write = 0.0
    while True:
        try:
            if not assigned:
                consumer.assign(start_positions(consumer, datetime.now(UTC)))
                assigned = True
            # o Flink regrava a chave a cada post da janela: chegam milhares de
            # mensagens por minuto, entao le em lote
            for msg in consumer.consume(num_messages=1000, timeout=1.0):
                if not msg.error():
                    apply_message(view, msg.topic(), msg.key(), msg.value())
            now = time.monotonic()
            if now - last_write >= args.interval:
                _write_atomic(view.snapshot(datetime.now(UTC)), args.output)
                last_write = now
        except Exception as exc:  # topico ainda nao criado, broker reiniciando
            logger.error("flink_export_cycle_failed", extra={"error": str(exc)})
            time.sleep(5)


if __name__ == "__main__":
    main()
