"""Servico ch-export: le as janelas do ClickHouse e regrava o historico.json.

A pagina /historico/ mostra o que o ClickHouse guardou: crescimento por termo
comparado com a media das ultimas 24 horas, o mapa de ritmo por hora e dia da
semana, e o volume do dia. Nada aqui recalcula janela: os numeros vem das
tabelas que o Flink alimenta.

Regras que este modulo garante (e os testes cobrem):
- so agregados: nenhum termo vem com did, uri ou texto, porque as tabelas do
  ClickHouse tambem nao guardam isso;
- campo sem dado vale null ou lista vazia, nunca zero inventado;
- crescimento so aparece com historico suficiente (MIN_HISTORY_HOURS), senao a
  pagina troca o rotulo para autores;
- a leitura e so SELECT: o servico nao escreve no banco.
"""

import argparse
import json
import logging
import os
import time
from datetime import UTC, datetime
from urllib.parse import urlencode
from urllib.request import Request, urlopen

logging.basicConfig(
    level=logging.INFO,
    format='{"ts":"%(asctime)s","level":"%(levelname)s","msg":"%(message)s"}',
)
logger = logging.getLogger("ch-export")

MAX_ITEMS = 10
MIN_HISTORY_HOURS = 3
HEATMAP_DAYS = 7
HEATMAP_HOURS = 24
SPARK_WINDOWS = 12
VOLUME_HOURS = 24


def query(clickhouse_url: str, sql: str) -> list[dict]:
    """Executa um SELECT e devolve as linhas como dicionarios."""
    parametros = urlencode({"query": sql + " FORMAT JSONEachRow"})
    url = f"{clickhouse_url.rstrip('/')}/?{parametros}"
    request = Request(url, headers={"Accept": "application/json"})
    with urlopen(request, timeout=20) as response:  # noqa: S310 - url interna
        body = response.read().decode("utf-8")
    rows = []
    for line in body.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            rows.append(json.loads(line))
        except ValueError:
            continue
    return rows


def _iso(value) -> str | None:
    """DateTime do ClickHouse ("2026-11-12 03:05:00") como ISO UTC."""
    if not isinstance(value, str) or not value:
        return None
    text = value.replace("T", " ").removesuffix("Z").split(".")[0]
    try:
        moment = datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None
    return moment.replace(tzinfo=UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _hours_of_data(clickhouse_url: str) -> float:
    rows = query(
        clickhouse_url,
        "SELECT dateDiff('second', min(window_start), max(window_start)) AS span "
        "FROM bluesky.trending_terms",
    )
    if not rows:
        return 0.0
    return float(rows[0].get("span") or 0) / 3600


def growth_items(clickhouse_url: str, limit: int = MAX_ITEMS) -> list[dict]:
    """Termos ordenados por crescimento contra a media das ultimas 24 horas.

    ratio = (autores na ultima hora + 5) / (media de autores na mesma hora dos
    ultimos 7 dias + 5). O +5 e o mesmo suavizador da especificacao: sem ele,
    um termo que nunca apareceu dividiria por zero e dominaria a lista.
    """
    sql = """
    WITH
      now() AS agora,
      (SELECT max(window_start) FROM bluesky.trending_terms) AS ultima
    SELECT
      term,
      kind,
      sumIf(authors, window_start >= ultima - INTERVAL 60 MINUTE) AS autores_1h,
      sumIf(posts, window_start >= ultima - INTERVAL 60 MINUTE) AS posts_1h,
      avgIf(authors, window_start < ultima - INTERVAL 60 MINUTE
            AND window_start >= ultima - INTERVAL 7 DAY) AS media_7d,
      groupArrayIf(authors, window_start >= ultima - INTERVAL 60 MINUTE) AS spark
    FROM bluesky.trending_terms
    WHERE window_start >= ultima - INTERVAL 7 DAY
    GROUP BY term, kind
    HAVING autores_1h > 0
    ORDER BY autores_1h DESC
    LIMIT 200
    """
    rows = query(clickhouse_url, sql)
    items = []
    for row in rows:
        autores = int(row.get("autores_1h") or 0)
        media = float(row.get("media_7d") or 0)
        ratio = round((autores + 5) / (media + 5), 2) if media else None
        spark = row.get("spark") or []
        items.append(
            {
                "term": row.get("term"),
                "kind": row.get("kind"),
                "authors": autores,
                "posts": int(row.get("posts_1h") or 0),
                "ratio": ratio,
                "spark": [int(v) for v in spark[-SPARK_WINDOWS:]],
            }
        )
    items.sort(key=lambda item: (-(item["ratio"] or 0), -item["authors"]))
    return items[:limit]


def heatmap(clickhouse_url: str) -> list[list[int]] | None:
    """Media de posts por hora, 7 linhas (segunda a domingo) por 24 colunas."""
    sql = """
    SELECT
      toDayOfWeek(window_start, 1) - 1 AS dia,
      toHour(window_start) AS hora,
      round(avg(posts), 1) AS media
    FROM bluesky.volume_minutes
    WHERE window_start >= now() - INTERVAL 7 DAY
    GROUP BY dia, hora
    ORDER BY dia, hora
    """
    rows = query(clickhouse_url, sql)
    if not rows:
        return None
    grid = [[0.0] * HEATMAP_HOURS for _ in range(HEATMAP_DAYS)]
    for row in rows:
        dia = int(row.get("dia") or 0)
        hora = int(row.get("hora") or 0)
        if 0 <= dia < HEATMAP_DAYS and 0 <= hora < HEATMAP_HOURS:
            grid[dia][hora] = float(row.get("media") or 0)
    if not any(any(cell for cell in line) for line in grid):
        return None
    return [[round(cell, 1) for cell in line] for line in grid]


def volume_hours(clickhouse_url: str) -> list[dict]:
    sql = f"""
    SELECT
      toStartOfHour(window_start) AS hora,
      sum(posts) AS posts
    FROM bluesky.volume_minutes
    WHERE window_start >= now() - INTERVAL {VOLUME_HOURS} HOUR
    GROUP BY hora
    ORDER BY hora
    """
    rows = query(clickhouse_url, sql)
    return [
        {"t": _iso(row.get("hora")), "posts": int(row.get("posts") or 0)}
        for row in rows
        if row.get("hora")
    ]


def totals(clickhouse_url: str) -> dict:
    sql = """
    SELECT
      (SELECT count() FROM bluesky.trending_terms) AS linhas_trending,
      (SELECT count() FROM bluesky.volume_minutes) AS linhas_volume,
      (SELECT sum(posts) FROM bluesky.volume_minutes) AS posts_total,
      (SELECT min(window_start) FROM bluesky.volume_minutes) AS desde
    """
    rows = query(clickhouse_url, sql)
    row = rows[0] if rows else {}
    return {
        "rows_trending": int(row.get("linhas_trending") or 0),
        "rows_volume": int(row.get("linhas_volume") or 0),
        "posts_total": int(row.get("posts_total") or 0),
        "since": _iso(row.get("desde")),
    }


def build(clickhouse_url: str, now: datetime | None = None) -> dict:
    now = now or datetime.now(UTC)
    horas = _hours_of_data(clickhouse_url)
    ranking = "growth" if horas >= MIN_HISTORY_HOURS else "authors"
    itens = growth_items(clickhouse_url)
    if ranking == "authors":
        for item in itens:
            item["ratio"] = None
        itens.sort(key=lambda item: (-item["authors"], item["term"]))
    return {
        "generated_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source": "clickhouse",
        "history_hours": round(horas, 1),
        "ranking": ranking,
        "trending": itens,
        "heatmap": heatmap(clickhouse_url),
        "volume": volume_hours(clickhouse_url),
        "totals": totals(clickhouse_url),
    }


def _write_atomic(payload: dict, output: str) -> None:
    directory = os.path.dirname(output) or "."
    os.makedirs(directory, exist_ok=True)
    tmp = f"{output}.tmp"
    with open(tmp, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, separators=(",", ":"))
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(tmp, output)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="historico.json a partir do ClickHouse"
    )
    parser.add_argument(
        "--clickhouse",
        default=os.environ.get("CLICKHOUSE_URL", "http://clickhouse:8123"),
    )
    parser.add_argument(
        "--output", default=os.environ.get("HISTORY_OUTPUT", "/public/historico.json")
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=float(os.environ.get("HISTORY_INTERVAL_S", "60")),
    )
    args = parser.parse_args()

    logger.info("ch_export_started", extra={"output": args.output})
    while True:
        started = time.monotonic()
        try:
            _write_atomic(build(args.clickhouse), args.output)
            logger.info("history_written", extra={"output": args.output})
        except Exception as exc:  # banco subindo, tabela ainda vazia
            logger.error("ch_export_cycle_failed", extra={"error": str(exc)})
        elapsed = time.monotonic() - started
        time.sleep(max(args.interval - elapsed, 10.0))


if __name__ == "__main__":
    main()
