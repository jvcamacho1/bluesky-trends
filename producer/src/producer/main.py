import asyncio
import logging
import random
import signal
import sys

import requests

from .avro_serde import AvroPostSerializer
from .cursor_store import CursorStore
from .filters import is_portuguese
from .jetstream_client import build_subscribe_url, subscribe
from .kafka_producer import KafkaPublisher
from .parsing import InvalidMessage, build_post_event, is_commit_create, parse_frame
from .settings import get_settings

logging.basicConfig(
    level=logging.INFO,
    format='{"ts":"%(asctime)s","level":"%(levelname)s","msg":"%(message)s"}',
)
logger = logging.getLogger("producer")

STOP = asyncio.Event()


def _handle_signal(*_):
    STOP.set()


class Stats:
    def __init__(self):
        self.received = 0
        self.published = 0
        self.filtered_lang = 0
        self.discarded_op = 0
        self.dlq = 0

    def summary(self) -> dict:
        return {
            "received": self.received,
            "published": self.published,
            "filtered_lang": self.filtered_lang,
            "discarded_op": self.discarded_op,
            "dlq": self.dlq,
        }


async def process_frame(frame, publisher, stats, cursor_store, cursor_state, settings):
    stats.received += 1
    try:
        payload = parse_frame(frame)
    except InvalidMessage as exc:
        stats.dlq += 1
        publisher.publish_dlq(frame, exc.stage, exc.reason)
        return

    seq = payload.get("seq")
    if isinstance(seq, int):
        cursor_state["seq"] = seq

    if not is_commit_create(payload):
        stats.discarded_op += 1
        return

    try:
        event = build_post_event(payload)
    except InvalidMessage as exc:
        stats.dlq += 1
        publisher.publish_dlq(frame, exc.stage, exc.reason)
        return

    if not is_portuguese(event.langs, settings.lang_primary_subtags):
        stats.filtered_lang += 1
        return

    publisher.publish_post(event.to_dict(), key=event.did)
    stats.published += 1

    if cursor_state["seq"] is not None and (
        stats.published % settings.cursor_persist_every == 0
    ):
        cursor_store.save(cursor_state["seq"])


async def read_file_frames(path: str):
    with open(path, "rb") as f:
        for line in f:
            line = line.strip()
            if line:
                yield line


async def run() -> int:
    settings = get_settings()
    cursor_store = CursorStore(settings.cursor_path)
    cursor_state: dict = {"seq": None}
    if settings.cursor_resume:
        cursor_state["seq"] = cursor_store.load()
        if cursor_state["seq"] is not None:
            logger.info("cursor_resumed", extra={"seq": cursor_state["seq"]})

    serializer = AvroPostSerializer(
        settings.schema_path,
        settings.schema_registry_url,
        settings.schema_subject,
        settings.schema_compatibility,
    )
    for attempt in range(30):
        try:
            serializer.register()
            break
        except requests.RequestException as exc:
            logger.warning(
                "schema_registry_unreachable",
                extra={"attempt": attempt + 1, "error": str(exc)},
            )
            await asyncio.sleep(2)
    else:
        logger.error("schema_registry_gave_up")
        return 1
    publisher = KafkaPublisher(settings, serializer)
    stats = Stats()

    backoff = 1.0
    exit_code = 0
    while not STOP.is_set():
        try:
            if settings.input_mode == "file":
                if cursor_state["seq"] is not None:
                    logger.info("file_mode_ignores_cursor")
                async for frame in read_file_frames(settings.input_file):
                    if STOP.is_set():
                        break
                    await process_frame(
                        frame,
                        publisher,
                        stats,
                        cursor_store,
                        cursor_state,
                        settings,
                    )
                break
            url = build_subscribe_url(
                settings.jetstream_url,
                settings.collections,
                settings.kinds,
                cursor_state["seq"],
            )
            async for frame in subscribe(url):
                if STOP.is_set():
                    break
                await process_frame(
                    frame,
                    publisher,
                    stats,
                    cursor_store,
                    cursor_state,
                    settings,
                )
                backoff = 1.0
        except Exception as exc:
            if STOP.is_set():
                break
            if settings.input_mode == "file":
                logger.error("file_mode_failed", extra={"error": str(exc)})
                exit_code = 1
                break
            logger.warning(
                "jetstream_disconnected",
                extra={"error": str(exc), "retry_in_s": backoff},
            )
            await asyncio.sleep(backoff + random.uniform(0, backoff * 0.1))
            backoff = min(backoff * 2, settings.reconnect_max_backoff_s)

    if cursor_state["seq"] is not None:
        cursor_store.save(cursor_state["seq"])
    publisher.flush(30)
    counters = publisher.counters
    logger.info("producer_stopped", extra={**stats.summary(), **counters})
    if stats.published > 0 and counters["delivered"] == 0:
        logger.error("no_messages_delivered", extra={"failed": counters["failed"]})
        return 1
    return exit_code


def main() -> None:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _handle_signal)
        except NotImplementedError:
            signal.signal(sig, _handle_signal)
    sys.exit(loop.run_until_complete(run()))


if __name__ == "__main__":
    main()
