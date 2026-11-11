"""Agregados publicos do painel: contagens em baldes de 5 minutos, 24 horas.

Regras que este modulo garante (e os testes cobrem):
- so agregados saem daqui: nenhum texto, did, uri ou rkey no public.json;
- post com rotulo proprio adulto ou termo da lista de bloqueio nao entra;
- cada autor conta uma vez por termo por janela (contar pessoas, nao posts);
- um termo so aparece com PUBLIC_MIN_AUTHORS autores distintos ou mais.
"""

import math
from collections import defaultdict
from datetime import UTC, datetime, timedelta

from .public_terms import (
    has_adult_self_label,
    is_blocked,
    normalize_hashtag,
    tokenize,
)

BUCKET_S = 300
BUCKETS_24H = 24 * 60 * 60 // BUCKET_S  # 288
SPARK_BUCKETS = 12  # ultima hora
MAX_TRENDING = 10
LATENCY_MAX_LAG_S = 600


def _parse_time(value: str) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def bucket_start(moment: datetime) -> datetime:
    return moment.replace(second=0, microsecond=0) - timedelta(
        seconds=moment.minute % 5 * 60
    )


class PublicAggregator:
    """Mantem em memoria as contagens das ultimas 24 horas."""

    def __init__(self, min_authors: int = 10, blocklist: tuple[str, ...] = ()):
        self.min_authors = min_authors
        self.blocklist = blocklist
        # term_key -> bucket -> set(did)
        self._authors: dict[tuple[str, str], dict[datetime, set[str]]] = defaultdict(
            lambda: defaultdict(set)
        )
        # term_key -> bucket -> posts
        self._posts: dict[tuple[str, str], dict[datetime, int]] = defaultdict(
            lambda: defaultdict(int)
        )
        self._volume: dict[datetime, int] = defaultdict(int)
        self._authors_1h: dict[datetime, set[str]] = defaultdict(set)
        self._latencies_ms: dict[datetime, list[float]] = defaultdict(list)
        self._first_seen: datetime | None = None
        self._last_seen: datetime | None = None

    # -- ingestao -------------------------------------------------------
    def add(self, event: dict, now: datetime | None = None) -> bool:
        """Adiciona um PostEvent. Retorna False se o post foi filtrado."""
        now = now or datetime.now(UTC)
        event_time = _parse_time(event.get("event_time", ""))
        did = event.get("did")
        if event_time is None or not isinstance(did, str) or not did:
            return False
        if event_time > now + timedelta(minutes=5):
            return False

        hashtags = [
            normalize_hashtag(h.get("tag", ""))
            for h in (event.get("hashtag_entities") or [])
            if isinstance(h, dict) and h.get("tag")
        ]
        hashtags = [h for h in hashtags if h]
        text = event.get("text") or ""

        if has_adult_self_label(event.get("self_labels")):
            return False
        if is_blocked(text, hashtags, self.blocklist):
            return False

        bucket = bucket_start(event_time)
        if bucket < bucket_start(now) - timedelta(seconds=BUCKET_S * BUCKETS_24H):
            return False

        terms: dict[tuple[str, str], None] = {}
        for tag in hashtags:
            terms[("hashtag", tag)] = None
        for token in tokenize(text):
            terms[("term", token)] = None

        for key in terms:
            self._authors[key][bucket].add(did)
            self._posts[key][bucket] += 1

        self._volume[bucket] += 1
        if bucket >= bucket_start(now) - timedelta(hours=1):
            self._authors_1h[bucket].add(did)
            ingest_time = _parse_time(event.get("ingest_time", ""))
            if ingest_time is not None:
                lag_s = (ingest_time - event_time).total_seconds()
                if 0 <= lag_s <= LATENCY_MAX_LAG_S:
                    self._latencies_ms[bucket].append(lag_s * 1000)

        self._first_seen = self._first_seen or now
        self._last_seen = now
        return True

    # -- manutencao -----------------------------------------------------
    def prune(self, now: datetime | None = None) -> None:
        now = now or datetime.now(UTC)
        oldest = bucket_start(now) - timedelta(seconds=BUCKET_S * BUCKETS_24H)
        for store in (self._authors, self._posts):
            for key in list(store):
                for bucket in [b for b in store[key] if b < oldest]:
                    del store[key][bucket]
                if not store[key]:
                    del store[key]
        for bucket in [b for b in self._volume if b < oldest]:
            del self._volume[bucket]
        for bucket in [b for b in self._authors_1h if b < oldest]:
            del self._authors_1h[bucket]
        for bucket in [b for b in self._latencies_ms if b < oldest]:
            del self._latencies_ms[bucket]

    # -- saida ----------------------------------------------------------
    def _last_hour_buckets(self, now: datetime) -> list[datetime]:
        current = bucket_start(now)
        return [current - timedelta(seconds=BUCKET_S * i) for i in range(SPARK_BUCKETS)]

    def trending(self, now: datetime | None = None) -> dict:
        now = now or datetime.now(UTC)
        hour_buckets = self._last_hour_buckets(now)
        items = []
        for (kind, term), buckets in self._authors.items():
            authors_last_hour = set()
            spark = []
            for bucket in reversed(hour_buckets):
                people = buckets.get(bucket, set())
                spark.append(len(people))
                authors_last_hour |= people
            if len(authors_last_hour) < self.min_authors:
                continue
            posts_last_hour = sum(
                self._posts[(kind, term)].get(bucket, 0) for bucket in hour_buckets
            )
            items.append(
                {
                    "term": term,
                    "kind": kind,
                    "authors": len(authors_last_hour),
                    "posts": posts_last_hour,
                    "ratio": None,
                    "spark": spark,
                }
            )
        items.sort(key=lambda item: (-item["authors"], -item["posts"], item["term"]))
        return {"ranking": "authors", "items": items[:MAX_TRENDING]}

    def volume_24h(self, now: datetime | None = None) -> list[dict]:
        now = now or datetime.now(UTC)
        current = bucket_start(now)
        points = []
        for i in range(BUCKETS_24H - 1, -1, -1):
            bucket = current - timedelta(seconds=BUCKET_S * i)
            posts = self._volume.get(bucket, 0)
            points.append(
                {
                    "t": bucket.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "posts_per_minute": round(posts / (BUCKET_S / 60), 2),
                }
            )
        return points

    def kpis(self, now: datetime | None = None) -> dict:
        now = now or datetime.now(UTC)
        hour_buckets = self._last_hour_buckets(now)
        current = bucket_start(now)
        posts_24h = sum(
            self._volume.get(current - timedelta(seconds=BUCKET_S * i), 0)
            for i in range(BUCKETS_24H)
        )
        authors_1h: set[str] = set()
        for bucket in hour_buckets:
            authors_1h |= self._authors_1h.get(bucket, set())
        latencies = [
            value
            for bucket in hour_buckets
            for value in self._latencies_ms.get(bucket, [])
        ]
        return {
            "posts_per_minute": round(
                sum(self._volume.get(b, 0) for b in hour_buckets)
                / (len(hour_buckets) * BUCKET_S / 60),
                2,
            ),
            "posts_24h": posts_24h,
            "authors_1h": len(authors_1h),
            "latency_p95_ms": _percentile(latencies, 95),
        }

    def snapshot(self, now: datetime | None = None) -> dict:
        now = now or datetime.now(UTC)
        self.prune(now)
        uptime = (
            int((now - self._first_seen).total_seconds()) if self._first_seen else None
        )
        return {
            "generated_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "kpis": self.kpis(now),
            "trending": self.trending(now),
            "volume_24h": self.volume_24h(now),
            "spikes": [],
            "week_heatmap": None,
            "pipeline": {
                "uptime_s": uptime,
                "events_per_s": None,
                "lag": None,
                "dlq_ratio": None,
            },
        }


def _percentile(values: list[float], pct: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    index = max(0, math.ceil(pct / 100 * len(ordered)) - 1)
    return round(ordered[index], 1)
