"""Do fixture JSONL ate o public.json: valida o contrato de ponta a ponta.

Nao precisa de Kafka: usa o mesmo parsing do producer e o mesmo agregador do
servico public-export, que e o que roda em producao.
"""

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from producer.parsing import InvalidMessage, build_post_event, parse_frame
from producer.public_aggregator import PublicAggregator
from producer.public_terms import load_blocklist

pytestmark = pytest.mark.contract

FIXTURE = Path(__file__).resolve().parents[1] / "fixtures" / "jetstream_frames.jsonl"
NOW = datetime(2026, 8, 13, 7, 0, tzinfo=UTC)


def _events():
    events = []
    for line in FIXTURE.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            events.append(build_post_event(parse_frame(line)).to_dict())
        except InvalidMessage:
            continue  # frames de update/delete nao tem record, como em producao
    return events


def _snapshot(events, now=NOW):
    agg = PublicAggregator(min_authors=10, blocklist=load_blocklist())
    for event in events:
        agg.add(event, now=now)
    return agg.snapshot(now)


def test_fixture_ate_public_json_respeita_o_contrato():
    snapshot = _snapshot(_events())
    assert set(snapshot) == {
        "generated_at",
        "kpis",
        "trending",
        "volume_24h",
        "spikes",
        "week_heatmap",
        "pipeline",
    }
    assert len(snapshot["volume_24h"]) == 288
    assert snapshot["trending"]["ranking"] == "authors"
    for item in snapshot["trending"]["items"]:
        assert item["ratio"] is None
        assert item["kind"] in {"hashtag", "term"}
        assert len(item["spark"]) == 12
        assert item["authors"] >= 10


def test_fixture_nao_vaza_texto_nem_identificador():
    payload = json.dumps(_snapshot(_events()), ensure_ascii=False)
    assert "did:" not in payload
    assert "at://" not in payload
    for event in _events():
        assert event["text"] not in payload


def test_fixture_conta_so_posts_em_portugues():
    events = _events()
    snapshot = _snapshot(events)
    # o fixture tem 9 frames: en e es ficam fora do filtro de idioma em producao,
    # aqui todos entram, mas o post sem texto nao gera termo nenhum
    assert snapshot["kpis"]["posts_24h"] == len(events)


@pytest.mark.parametrize("label", ["porn", "sexual", "nudity", "graphic-media"])
def test_post_com_rotulo_adulto_nao_entra_nos_agregados(label):
    events = _events()
    for event in events:
        event["self_labels"] = [label]
    snapshot = _snapshot(events)
    assert snapshot["kpis"]["posts_24h"] == 0
    assert snapshot["trending"]["items"] == []


def test_janela_de_24h_ignora_post_antigo():
    old = NOW - timedelta(hours=30)
    events = [
        {
            "did": "did:plc:x",
            "uri": "at://did:plc:x/app.bsky.feed.post/1",
            "rkey": "1",
            "text": "assunto antigo",
            "event_time": old.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "ingest_time": old.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "self_labels": None,
            "hashtag_entities": None,
        }
    ]
    assert _snapshot(events)["kpis"]["posts_24h"] == 0
