"""Contrato do public.json e teste de nginx das rotas publicas."""

import re
from datetime import UTC, datetime, timedelta
from pathlib import Path

from producer.public_aggregator import PublicAggregator
from producer.public_terms import load_blocklist

REPO_ROOT = Path(__file__).resolve().parents[3]
NGINX_CONF = REPO_ROOT / "deploy" / "nginx-bluesky-trends.conf"
NOW = datetime(2026, 11, 11, 20, 0, tzinfo=UTC)


def _event(did: str, text: str, tags=(), minutes_ago: int = 1):
    moment = (NOW - timedelta(minutes=minutes_ago)).strftime("%Y-%m-%dT%H:%M:%SZ")
    return {
        "did": did,
        "uri": f"at://{did}/app.bsky.feed.post/x",
        "rkey": "x",
        "text": text,
        "event_time": moment,
        "ingest_time": moment,
        "self_labels": None,
        "hashtag_entities": [{"tag": t, "start": 0, "end": 1} for t in tags],
    }


def _snapshot():
    agg = PublicAggregator(min_authors=10, blocklist=load_blocklist())
    for i in range(15):
        agg.add(_event(f"did:plc:{i}", "assunto do momento", ["assunto"]), now=NOW)
    return agg.snapshot(NOW)


def test_contrato_do_public_json():
    data = _snapshot()
    assert set(data) == {
        "generated_at",
        "kpis",
        "trending",
        "volume_24h",
        "spikes",
        "week_heatmap",
        "pipeline",
    }
    assert set(data["kpis"]) == {
        "posts_per_minute",
        "posts_24h",
        "authors_1h",
        "latency_p95_ms",
    }
    assert data["trending"]["ranking"] in {"authors", "growth"}
    for item in data["trending"]["items"]:
        assert set(item) == {"term", "kind", "authors", "posts", "ratio", "spark"}
        assert item["kind"] in {"hashtag", "term"}
        assert len(item["spark"]) == 12
    assert len(data["volume_24h"]) == 288
    assert set(data["volume_24h"][0]) == {"t", "posts_per_minute"}
    assert data["generated_at"].endswith("Z")
    assert set(data["pipeline"]) == {"uptime_s", "events_per_s", "lag", "dlq_ratio"}


def test_nginx_interno_tem_auth_e_a_raiz_e_publica():
    conf = NGINX_CONF.read_text(encoding="utf-8")

    interno = re.search(r"location /interno/ \{(.*?)\n    \}", conf, re.S)
    assert interno, "falta o bloco location /interno/"
    assert "auth_basic" in interno.group(1)
    assert "auth_basic_user_file" in interno.group(1)

    raiz = re.search(r"location / \{(.*?)\n    \}", conf, re.S)
    assert raiz, "falta o bloco location /"
    assert "auth_basic" not in raiz.group(1)

    # data.json so pode aparecer dentro de /interno/
    for match in re.finditer(r"location ([^{]+) \{(.*?)\n    \}", conf, re.S):
        name, body = match.group(1).strip(), match.group(2)
        if "data.json" in body:
            assert name.lstrip("= ").startswith("/interno"), (
                f"data.json servido em {name}"
            )


def test_public_json_tem_cache_control():
    conf = NGINX_CONF.read_text(encoding="utf-8")
    block = re.search(r"location = /public\.json \{(.*?)\n    \}", conf, re.S)
    assert block, "falta o bloco location = /public.json"
    assert "public, max-age=30" in block.group(1)
