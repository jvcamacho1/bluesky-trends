import logging

from confluent_kafka import Producer

from .settings import Settings

logger = logging.getLogger(__name__)

PRODUCER_CONFIG_KEYS = (
    "enable.idempotence",
    "acks",
    "retries",
    "delivery.timeout.ms",
    "request.timeout.ms",
    "linger.ms",
    "compression.type",
    "client.id",
    "bootstrap.servers",
)


class KafkaPublisher:
    def __init__(self, settings: Settings, serializer):
        self.settings = settings
        self.serializer = serializer
        self.producer = Producer(
            {
                "bootstrap.servers": settings.kafka_bootstrap_servers,
                "client.id": settings.kafka_client_id,
                "enable.idempotence": True,
                "acks": "all",
                "retries": settings.kafka_message_max_retries,
                "linger.ms": settings.kafka_linger_ms,
                "compression.type": settings.kafka_compression,
                "socket.keepalive.enable": True,
            }
        )
        self._delivered = 0
        self._failed = 0

    def _on_delivery(self, err, msg):
        if err is not None:
            self._failed += 1
            logger.error(
                "delivery_failed",
                extra={"topic": msg.topic(), "error": str(err)},
            )
        else:
            self._delivered += 1

    def publish_post(self, event: dict, key: str) -> None:
        value = self.serializer.serialize(event)
        self.producer.produce(
            self.settings.kafka_topic,
            key=key.encode("utf-8"),
            value=value,
            on_delivery=self._on_delivery,
        )
        self._maybe_poll()

    def publish_dlq(self, raw: bytes | str, stage: str, reason: str) -> None:
        if isinstance(raw, str):
            raw = raw.encode("utf-8", errors="replace")
        self.producer.produce(
            self.settings.kafka_dlq_topic,
            key=None,
            value=raw,
            headers={"error.stage": stage.encode(), "error.reason": reason.encode()},
            on_delivery=self._on_delivery,
        )
        self._maybe_poll()

    def _maybe_poll(self) -> None:
        self.producer.poll(0)

    def flush(self, timeout: float = 30.0) -> int:
        return self.producer.flush(timeout)

    @property
    def counters(self) -> dict:
        return {"delivered": self._delivered, "failed": self._failed}
