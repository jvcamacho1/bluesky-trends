import asyncio
import logging
from collections.abc import AsyncIterator
from urllib.parse import urlencode

import websockets

logger = logging.getLogger(__name__)


def build_subscribe_url(
    base_url: str,
    collections: list[str],
    kinds: list[str],
    cursor: int | None = None,
) -> str:
    params: list[tuple[str, str]] = []
    for collection in collections:
        params.append(("collections", collection))
    for kind in kinds:
        params.append(("kinds", kind))
    if cursor is not None:
        params.append(("cursor", str(cursor)))
    separator = "&" if "?" in base_url else "?"
    return f"{base_url}{separator}{urlencode(params)}"


async def subscribe(
    url: str,
    stall_timeout_s: float = 90.0,
) -> AsyncIterator[bytes]:
    async with websockets.connect(
        url,
        subprotocols=["xrpc.v1.json"],
        ping_interval=20,
        ping_timeout=20,
    ) as websocket:
        logger.info("jetstream_connected", extra={"url": url})
        while True:
            frame = await asyncio.wait_for(websocket.recv(), timeout=stall_timeout_s)
            yield frame
