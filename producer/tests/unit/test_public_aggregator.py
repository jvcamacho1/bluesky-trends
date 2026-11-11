"""Agregados publicos: minimo de autores, filtro adulto, privacidade e ranking."""

import json
from datetime import UTC, datetime, timedelta

from producer.public_aggregator import PublicAggregator
from producer.public_terms import load_blocklist

NOW = datetime(2026, 11, 11, 20, 0, tzinfo=UTC)


def _event(did: str, text: str, tags=(), minutes_ago: int = 1, labels=None):
    moment = (NOW - timedelta(minutes=minutes_ago)).strftime("%Y-%m-%dT%H:%M:%S.000Z")
    return {
        "did": did,
        "uri": f"at://{did}/app.bsky.feed.post/abc",
        "rkey": "abc",
        "text": text,
        "langs": ["pt-BR"],
        "event_time": moment,
        "ingest_time": moment,
        "self_labels": labels,
        "hashtag_entities": [{"tag": tag, "start": 0, "end": 1} for tag in tags],
    }


def _aggregator():
    return PublicAggregator(min_authors=10, blocklist=load_blocklist())


def test_nove_autores_nao_aparece():
    agg = _aggregator()
    for i in range(9):
        agg.add(_event(f"did:plc:{i}", "assunto repetido", ["assunto"]), now=NOW)
    terms = [item["term"] for item in agg.trending(NOW)["items"]]
    assert "assunto" not in terms


def test_dez_autores_aparece():
    agg = _aggregator()
    for i in range(10):
        agg.add(_event(f"did:plc:{i}", "assunto repetido", ["assunto"]), now=NOW)
    items = agg.trending(NOW)["items"]
    assert any(item["term"] == "assunto" and item["authors"] == 10 for item in items)


def test_mesmo_autor_repetido_conta_uma_vez():
    agg = _aggregator()
    for _ in range(30):
        agg.add(_event("did:plc:mesmo", "assunto repetido", ["assunto"]), now=NOW)
    for i in range(9):
        agg.add(_event(f"did:plc:{i}", "assunto repetido", ["assunto"]), now=NOW)
    items = agg.trending(NOW)["items"]
    assert any(item["term"] == "assunto" and item["authors"] == 10 for item in items)


def test_rotulo_proprio_adulto_fica_fora_de_todos_os_agregados():
    agg = _aggregator()
    for i in range(20):
        agg.add(
            _event(f"did:plc:{i}", "assunto repetido", ["assunto"], labels=["porn"]),
            now=NOW,
        )
    snapshot = agg.snapshot(NOW)
    assert snapshot["trending"]["items"] == []
    assert snapshot["kpis"]["posts_24h"] == 0


def test_termo_da_lista_de_bloqueio_fica_fora():
    agg = _aggregator()
    for i in range(20):
        agg.add(_event(f"did:plc:{i}", "assunto pornografia", ["assunto"]), now=NOW)
    snapshot = agg.snapshot(NOW)
    assert snapshot["trending"]["items"] == []
    assert snapshot["kpis"]["posts_24h"] == 0


def test_privacidade_nenhum_dado_bruto_no_public_json():
    agg = _aggregator()
    for i in range(12):
        agg.add(
            _event(f"did:plc:{i}", "texto do post nao pode sair", ["assunto"]),
            now=NOW,
        )
    payload = json.dumps(agg.snapshot(NOW), ensure_ascii=False)

    def walk(node):
        if isinstance(node, dict):
            for key, value in node.items():
                assert key not in {"did", "uri", "rkey", "text"}, (
                    f"chave proibida: {key}"
                )
                walk(value)
        elif isinstance(node, list):
            for item in node:
                walk(item)
        elif isinstance(node, str):
            assert not node.startswith(("did:", "at://")), f"valor proibido: {node}"
            assert "nao pode sair" not in node

    walk(json.loads(payload))


def test_sem_historico_ranking_authors_e_ratio_nulo():
    agg = _aggregator()
    for i in range(12):
        agg.add(_event(f"did:plc:{i}", "assunto", ["assunto"]), now=NOW)
    trending = agg.trending(NOW)
    assert trending["ranking"] == "authors"
    assert all(item["ratio"] is None for item in trending["items"])


def test_volume_24h_tem_288_pontos():
    agg = _aggregator()
    agg.add(_event("did:plc:1", "assunto", ["assunto"]), now=NOW)
    assert len(agg.volume_24h(NOW)) == 288


def test_spark_tem_12_baldes():
    agg = _aggregator()
    for i in range(12):
        agg.add(_event(f"did:plc:{i}", "assunto", ["assunto"]), now=NOW)
    item = agg.trending(NOW)["items"][0]
    assert len(item["spark"]) == 12


def test_post_antigo_fora_da_janela_nao_entra():
    agg = _aggregator()
    for i in range(12):
        agg.add(
            _event(f"did:plc:{i}", "assunto", ["assunto"], minutes_ago=60 * 30), now=NOW
        )
    assert agg.snapshot(NOW)["kpis"]["posts_24h"] == 0
