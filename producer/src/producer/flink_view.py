"""Visao publica das janelas do Flink (fase 2): flink.json.

Le o que os jobs SQL gravam nos topicos upsert-kafka e monta os agregados que
a pagina /flink/ mostra. Nada aqui recalcula janela: o numero de autores de
cada termo e o COUNT(DISTINCT did) que o Flink fechou.

Regras que este modulo garante (e os testes cobrem):
- so agregados: os topicos de saida do Flink nao carregam did, uri, rkey nem
  texto, e o arquivo gerado tambem nao;
- um termo so aparece com FLINK_MIN_AUTHORS autores distintos na janela;
- termo da lista de bloqueio, palavra vazia, pedaco de link ou de mencao nao
  aparece (o Flink tokeniza sem tirar URL nem @, entao o filtro e aqui);
- minuto ou janela que nao existe no topico vira null, nunca zero inventado;
- janela ainda aberta nao aparece: o Flink regrava a mesma chave enquanto
  chegam posts daquela janela (upsert), entao so conta a janela cujo fim ja
  passou do watermark com folga (SETTLE). Para cada chave vale o ultimo valor.
"""

from datetime import UTC, datetime, timedelta

from .public_terms import STOPWORDS_PT, _fold

SLIDE = timedelta(minutes=1)
WINDOW = timedelta(minutes=5)
SPARK_WINDOWS = 12
MAX_ITEMS = 10
VOLUME_MINUTES = 180
MAX_TERM_LEN = 30
# watermark de 2 minutos no job + 1 minuto de folga para a emissao chegar
SETTLE = timedelta(minutes=3)

# pedacos que sobram quando o Flink tira a pontuacao de um link ou de um @
_NOISE_PREFIXES = ("http", "www")
_NOISE_PARTS = ("bskysocial", "bskyapp")
# dominio colado: "lupagrupos.com" vira "lupagruposcom" depois da limpeza
_DOMAIN_SUFFIXES = ("com", "combr", "net", "org", "orgbr", "gov", "govbr")


def parse_flink_time(value) -> datetime | None:
    """Timestamp do formato JSON do Flink ("2026-11-12 03:05:00" ou com
    fracao/"T"/"Z") como datetime UTC. Lixo vira None."""
    if not isinstance(value, str) or not value:
        return None
    text = value.strip().replace("T", " ").removesuffix("Z")
    text = text.split(".")[0]
    try:
        parsed = datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None
    return parsed.replace(tzinfo=UTC)


def _iso(moment: datetime) -> str:
    return moment.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def is_noise(term: str) -> bool:
    if len(term) > MAX_TERM_LEN:
        return True
    if term.startswith(_NOISE_PREFIXES):
        return True
    if len(term) >= 8 and term.endswith(_DOMAIN_SUFFIXES):
        return True
    return any(part in term for part in _NOISE_PARTS)


class FlinkView:
    """Guarda em memoria as ultimas janelas do Flink e monta o flink.json."""

    def __init__(
        self,
        min_authors: int = 10,
        blocklist: tuple[str, ...] = (),
        volume_minutes: int = VOLUME_MINUTES,
    ):
        self.min_authors = min_authors
        self.blocklist = blocklist
        self.volume_minutes = volume_minutes
        # window_end -> (kind, term) -> (authors, posts)
        self._trending: dict[datetime, dict[tuple[str, str], tuple[int, int]]] = {}
        # window_start -> posts (TUMBLE de 1 minuto)
        self._volume: dict[datetime, int] = {}

    # -- ingestao -------------------------------------------------------
    def add_trending(self, key: dict | None, value: dict | None) -> bool:
        """Uma linha de bluesky.trending.v1. value None e tombstone."""
        source = value if isinstance(value, dict) else key
        if not isinstance(source, dict):
            return False
        start = parse_flink_time(source.get("window_start"))
        kind = source.get("kind")
        term = source.get("term")
        if start is None or kind not in {"hashtag", "term"}:
            return False
        if not isinstance(term, str) or not term:
            return False
        end = parse_flink_time(source.get("window_end")) or start + WINDOW
        if value is None:
            bucket = self._trending.get(end)
            if bucket is not None:
                bucket.pop((kind, term), None)
            return True
        authors = value.get("authors")
        posts = value.get("posts")
        if not isinstance(authors, int) or not isinstance(posts, int):
            return False
        self._trending.setdefault(end, {})[(kind, term)] = (authors, posts)
        return True

    def add_volume(self, key: dict | None, value: dict | None) -> bool:
        """Uma linha de bluesky.volume.v1. value None e tombstone."""
        source = value if isinstance(value, dict) else key
        if not isinstance(source, dict):
            return False
        start = parse_flink_time(source.get("window_start"))
        if start is None:
            return False
        if value is None:
            self._volume.pop(start, None)
            return True
        posts = value.get("posts")
        if not isinstance(posts, int):
            return False
        self._volume[start] = posts
        return True

    def prune(self, now: datetime) -> None:
        """Descarta o que a pagina nao mostra mais, para a memoria nao crescer."""
        keep_trending = now - timedelta(minutes=SPARK_WINDOWS + 10)
        keep_volume = now - timedelta(minutes=self.volume_minutes + 10)
        for end in [e for e in self._trending if e < keep_trending]:
            del self._trending[end]
        for start in [s for s in self._volume if s < keep_volume]:
            del self._volume[start]

    # -- filtros --------------------------------------------------------
    def _visible(self, kind: str, term: str) -> bool:
        folded = _fold(term)
        if any(blocked in folded for blocked in self.blocklist):
            return False
        if kind == "term" and folded in STOPWORDS_PT:
            return False
        return not is_noise(folded)

    # -- saida ----------------------------------------------------------
    def _latest_window(self, now: datetime) -> datetime | None:
        ends = [e for e in self._trending if e <= now - SETTLE]
        return max(ends) if ends else None

    def _top(self, kind: str, end: datetime) -> list[dict]:
        window = self._trending.get(end, {})
        rows = [
            (term, authors, posts)
            for (k, term), (authors, posts) in window.items()
            if k == kind and authors >= self.min_authors and self._visible(k, term)
        ]
        rows.sort(key=lambda row: (-row[1], -row[2], row[0]))
        items = []
        for term, authors, posts in rows[:MAX_ITEMS]:
            spark: list[int | None] = []
            for back in range(SPARK_WINDOWS - 1, -1, -1):
                past = self._trending.get(end - back * SLIDE)
                if past is None:
                    spark.append(None)
                else:
                    spark.append(past.get((kind, term), (0, 0))[0])
            items.append(
                {"term": term, "authors": authors, "posts": posts, "spark": spark}
            )
        return items

    def _volume_series(self, now: datetime) -> list[dict]:
        settled = [s for s in self._volume if s + SLIDE <= now - SETTLE]
        if not settled:
            return []
        last = max(settled)
        first = last - timedelta(minutes=self.volume_minutes - 1)
        series = []
        moment = first
        while moment <= last:
            series.append({"t": _iso(moment), "posts": self._volume.get(moment)})
            moment += SLIDE
        return series

    def _kpis(self, volume: list[dict]) -> dict:
        known = [p for p in volume if p["posts"] is not None]
        last_hour = [p["posts"] for p in volume[-60:] if p["posts"] is not None]
        peak = max(known, key=lambda p: p["posts"]) if known else None
        return {
            "posts_last_minute": known[-1]["posts"] if known else None,
            "posts_last_hour": sum(last_hour) if last_hour else None,
            "peak_minute": {"t": peak["t"], "posts": peak["posts"]} if peak else None,
        }

    def snapshot(self, now: datetime) -> dict:
        self.prune(now)
        end = self._latest_window(now)
        volume = self._volume_series(now)
        # atraso entre agora e o fim da janela mais nova ja assentada
        delays = [(now - end).total_seconds()] if end is not None else []
        return {
            "generated_at": _iso(now),
            "min_authors": self.min_authors,
            "window": (
                {"start": _iso(end - WINDOW), "end": _iso(end)}
                if end is not None
                else None
            ),
            "kpis": {
                **self._kpis(volume),
                "event_delay_s": round(min(delays)) if delays else None,
            },
            "hashtags": self._top("hashtag", end) if end is not None else [],
            "terms": self._top("term", end) if end is not None else [],
            "volume": volume,
        }
