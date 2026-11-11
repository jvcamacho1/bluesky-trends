"""Loop do exportador do dashboard: roda o export a cada intervalo e troca o
data.json de forma atomica.

Roda como servico do Compose (dashboard-export), sem depender de systemd nem de
Python instalado no host. Em caso de falha do export, o arquivo anterior fica
intacto e o servico tenta de novo no proximo ciclo.
"""

import argparse
import json
import logging
import os
import time

from .consume_check import consume
from .export_dashboard import build_dashboard

logging.basicConfig(
    level=logging.INFO,
    format='{"ts":"%(asctime)s","level":"%(levelname)s","msg":"%(message)s"}',
)
logger = logging.getLogger("dashboard-export")


def export_once(
    bootstrap: str, registry: str, topic: str, limit: int, output: str
) -> int:
    messages = consume(bootstrap, registry, topic, limit, timeout_s=60.0)
    if not messages:
        logger.warning("no_messages_consumed", extra={"topic": topic})
        return 0

    dashboard = build_dashboard(messages)
    directory = os.path.dirname(output) or "."
    os.makedirs(directory, exist_ok=True)
    tmp = f"{output}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(dashboard, f, ensure_ascii=False, indent=2)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, output)
    logger.info(
        "dashboard_written",
        extra={"total_posts": dashboard["total_posts"], "output": output},
    )
    return dashboard["total_posts"]


def main() -> None:
    parser = argparse.ArgumentParser(description="Exporta data.json em laco")
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
    parser.add_argument("--limit", type=int, default=500)
    parser.add_argument(
        "--output", default=os.environ.get("DASHBOARD_OUTPUT", "/public/data.json")
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=float(os.environ.get("EXPORT_INTERVAL_S", "300")),
    )
    args = parser.parse_args()

    logger.info(
        "dashboard_export_started",
        extra={"output": args.output, "interval_s": args.interval},
    )
    while True:
        started = time.monotonic()
        try:
            export_once(
                args.bootstrap, args.registry, args.topic, args.limit, args.output
            )
        except Exception as exc:  # o laco nao morre por uma falha de ciclo
            logger.error("dashboard_export_failed", extra={"error": str(exc)})
        elapsed = time.monotonic() - started
        time.sleep(max(args.interval - elapsed, 5.0))


if __name__ == "__main__":
    main()
