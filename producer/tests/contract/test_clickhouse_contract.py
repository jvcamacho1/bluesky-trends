"""Contrato da fase 3: ClickHouse, ch-sink, ch-export e a pagina /historico/.

Nao sobe banco: le os arquivos e confere as regras que, se quebrarem, so
aparecem em producao (retencao, dedupe, privacidade, orcamento, rotas).
"""

import json
import re
from datetime import UTC, datetime
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[3]
COMPOSE = REPO_ROOT / "docker-compose.yml"
COMPOSE_CH = REPO_ROOT / "docker-compose.clickhouse.yml"
INIT_SQL = REPO_ROOT / "clickhouse" / "init.sql"
LIMITS_XML = REPO_ROOT / "clickhouse" / "limits.xml"
NGINX = REPO_ROOT / "deploy" / "nginx-bluesky-trends.conf"
PAGE = REPO_ROOT / "docs" / "historico.html"


def test_tabelas_tem_ttl_e_dedupe():
    sql = INIT_SQL.read_text(encoding="utf-8")
    assert sql.count("ENGINE = ReplacingMergeTree") == 2
    assert "TTL window_start + INTERVAL 7 DAY" in sql
    assert "TTL window_start + INTERVAL 30 DAY" in sql
    # a chave de ordenacao inclui janela e termo: reenviar substitui a linha
    assert "ORDER BY (window_start, kind, term)" in sql
    assert "ORDER BY (window_start)" in sql


def test_clickhouse_tem_limite_de_memoria_abaixo_do_container():
    xml = LIMITS_XML.read_text(encoding="utf-8")
    limite = int(re.search(r"<max_server_memory_usage>(\d+)<", xml).group(1))
    compose = yaml.safe_load(COMPOSE_CH.read_text(encoding="utf-8"))
    container = compose["services"]["clickhouse"]["deploy"]["resources"]["limits"][
        "memory"
    ]
    assert container == "1g"
    assert limite < 1024 * 1024 * 1024, (
        "max_server_memory_usage precisa ficar abaixo de 1 GB"
    )


def test_stack_inteira_dentro_do_orcamento():
    compose = yaml.safe_load(COMPOSE.read_text(encoding="utf-8"))
    ch = yaml.safe_load(COMPOSE_CH.read_text(encoding="utf-8"))
    servicos = {**compose["services"], **ch["services"]}
    memoria = 0.0
    cpus = 0.0
    for nome, servico in servicos.items():
        limites = ((servico.get("deploy") or {}).get("resources") or {}).get(
            "limits"
        ) or {}
        valor = str(limites.get("memory", "0"))
        memoria += int(valor[:-1]) / 1024 if valor.endswith("m") else int(valor[:-1])
        cpus += float(limites.get("cpus", 0))
        for porta in servico.get("ports", []) or []:
            assert str(porta).startswith("127.0.0.1:"), f"{nome}: {porta}"
    assert memoria <= 7, f"memoria total {memoria:.2f} GB"
    assert cpus <= 3.0, f"cpus totais {cpus:.2f}"


def test_ch_sink_so_grava_agregados():
    fonte = (REPO_ROOT / "producer" / "src" / "producer" / "ch_sink.py").read_text(
        encoding="utf-8"
    )
    # as colunas gravadas sao exatamente as agregadas
    assert '"window_start", "window_end", "term", "kind", "authors", "posts"' in fonte
    assert '"window_start", "window_end", "posts"' in fonte
    for proibido in ("did", "uri", "rkey", "text"):
        assert f'"{proibido}"' not in fonte.split("INSERT INTO")[0]


def test_ch_export_le_so_agregados_e_nao_inventa_zero():
    fonte = (REPO_ROOT / "producer" / "src" / "producer" / "ch_export.py").read_text(
        encoding="utf-8"
    )
    assert "FORMAT JSONEachRow" in fonte
    # crescimento so com historico suficiente
    assert "MIN_HISTORY_HOURS" in fonte
    assert 'ranking = "growth" if horas >= MIN_HISTORY_HOURS else "authors"' in fonte
    # sem dado vira None, nunca zero
    assert "if not rows:\n        return None" in fonte


def test_historico_json_nao_vaza_dado_bruto():

    def fake_query(_url, sql):
        if "dateDiff" in sql:
            return [{"span": 4 * 3600}]
        if "toDayOfWeek" in sql:
            return [{"dia": 0, "hora": 3, "media": 40.0}]
        if "toStartOfHour" in sql:
            return [{"hora": "2026-11-12 03:00:00", "posts": 40}]
        if "count()" in sql:
            return [
                {
                    "linhas_trending": 1,
                    "linhas_volume": 1,
                    "posts_total": 40,
                    "desde": "2026-11-12 03:00:00",
                }
            ]
        if "trending_terms" in sql:
            return [
                {
                    "term": "exemplo",
                    "kind": "term",
                    "autores_1h": 12,
                    "posts_1h": 20,
                    "media_7d": 4.0,
                    "spark": [1, 2, 3],
                }
            ]
        if "volume_minutes" in sql and "toStartOfHour" in sql:
            return [{"hora": "2026-11-12 03:00:00", "posts": 40}]
        if "toDayOfWeek" in sql:
            return [{"dia": 0, "hora": 3, "media": 40.0}]
        if "dateDiff" in sql:
            return [{"span": 4 * 3600}]
        return [
            {
                "linhas_trending": 1,
                "linhas_volume": 1,
                "posts_total": 40,
                "desde": "2026-11-12 03:00:00",
            }
        ]

    import producer.ch_export as modulo

    original = modulo.query
    modulo.query = fake_query
    try:
        dados = modulo.build(
            "http://clickhouse:8123", datetime(2026, 11, 12, 4, tzinfo=UTC)
        )
    finally:
        modulo.query = original

    texto = json.dumps(dados, ensure_ascii=False)
    assert "did:" not in texto and "at://" not in texto
    assert '"text"' not in texto
    assert dados["ranking"] == "growth"
    assert dados["trending"][0]["ratio"] is not None
    assert len(dados["heatmap"]) == 7 and len(dados["heatmap"][0]) == 24


def test_nginx_serve_historico_aberto_e_interno_com_senha():
    conf = NGINX.read_text(encoding="utf-8")
    historico = re.search(r"location = /historico/ \{(.*?)\n    \}", conf, re.S)
    assert historico, "falta o bloco location = /historico/"
    assert "auth_basic" not in historico.group(1)
    assert "public, max-age=300" in historico.group(1)

    json_block = re.search(r"location = /historico\.json \{(.*?)\n    \}", conf, re.S)
    assert json_block, "falta o bloco location = /historico.json"
    assert "public, max-age=30" in json_block.group(1)


def test_pagina_historico_respeita_as_regras_da_pagina():
    html = PAGE.read_text(encoding="utf-8")
    assert "innerHTML" not in html
    assert "/historico.json" in html
    # mesma identidade visual dos outros paineis
    assert "Instrument Serif" in html
    assert "--accent: #1185fe" in html
    assert 'href="/flink/"' in html and 'href="/"' in html
    assert 'aria-current="page"' in html


def test_deploy_publica_as_tres_paginas():
    script = (REPO_ROOT / "deploy" / "deploy.sh").read_text(encoding="utf-8")
    for pagina in ("public.html", "flink.html", "historico.html"):
        assert pagina in script
