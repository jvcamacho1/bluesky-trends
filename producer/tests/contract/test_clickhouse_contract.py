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
    # o ClickHouse usa o menor entre o valor fixo e ratio x memoria do container;
    # com 0.5 o teto real caia para 512 MiB, abaixo do que o servidor ocupa parado
    ratio = float(
        re.search(r"<max_server_memory_usage_to_ram_ratio>([\d.]+)<", xml).group(1)
    )
    assert ratio * 1024 * 1024 * 1024 >= limite, "o ratio corta o limite fixo"
    # logs de sistema ligados por padrao estouram a memoria ao compactar
    for log in ("metric_log", "trace_log", "error_log", "processors_profile_log"):
        assert f'<{log} remove="1"/>' in xml, log


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


def test_ch_sink_descarta_janela_no_futuro():
    from producer.ch_sink import is_future

    agora = datetime(2026, 11, 17, 19, 0, tzinfo=UTC)
    assert not is_future("2026-11-17 18:59:00", agora)
    assert not is_future("2026-11-17 19:02:00", agora)  # dentro da folga
    assert is_future("2026-11-17 19:03:00", agora)
    assert is_future("2026-11-18 07:13:00", agora)


def test_ch_export_ignora_janela_no_futuro():
    # uma janela 12 h a frente nao pode virar a "ultima hora" do crescimento
    import producer.ch_export as modulo

    consultas = []

    def fake_query(_url, sql):
        consultas.append(sql)
        return []

    original = modulo.query
    modulo.query = fake_query
    try:
        modulo.build("http://clickhouse:8123", datetime(2026, 11, 17, tzinfo=UTC))
    finally:
        modulo.query = original

    filtro = "window_start <= now() + INTERVAL 2 MINUTE"
    for sql in consultas:
        if "count()" in sql:
            continue  # totais contam o que o banco tem
        assert filtro in sql, sql
    crescimento = next(sql for sql in consultas if "AS ultima" in sql)
    assert f"WHERE {filtro}) AS ultima" in crescimento


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


def test_heatmap_agrupa_no_horario_de_brasilia():
    # a pagina diz "horario de Brasilia"; a coluna e DateTime('UTC')
    import producer.ch_export as modulo

    consultas = []

    def fake_query(_url, sql):
        consultas.append(sql)
        return [{"dia": 0, "hora": 21, "media": 10.0}]

    original = modulo.query
    modulo.query = fake_query
    try:
        grade = modulo.heatmap("http://clickhouse:8123")
    finally:
        modulo.query = original

    sql = consultas[0]
    assert "toDayOfWeek(toTimeZone(window_start, 'America/Sao_Paulo'), 1) AS dia" in sql
    # modo 1 ja e segunda = 0; "- 1" jogava a segunda para -1
    assert ", 1) - 1" not in sql
    assert "toHour(toTimeZone(window_start, 'America/Sao_Paulo'))" in sql
    assert grade[0][21] == 10.0


def _crescimento(janelas: dict, linhas: list[dict]) -> list[dict]:
    import producer.ch_export as modulo

    def fake_query(_url, sql):
        return [janelas] if "uniqExactIf" in sql else linhas

    original = modulo.query
    modulo.query = fake_query
    try:
        return modulo.growth_items("http://clickhouse:8123")
    finally:
        modulo.query = original


def test_crescimento_compara_media_por_janela():
    # 60 janelas na hora, 600 no resto da semana. "estavel" tem 10 autores por
    # janela nos dois periodos: ratio 1, nao ~60 (soma da hora / uma janela).
    janelas = {"janelas_1h": 60, "janelas_base": 600}
    linhas = [
        {"term": "estavel", "kind": "term", "autores_1h": 600, "autores_base": 6000},
        {"term": "subindo", "kind": "term", "autores_1h": 900, "autores_base": 600},
        {"term": "inedito", "kind": "term", "autores_1h": 300, "autores_base": 0},
    ]
    itens = {item["term"]: item for item in _crescimento(janelas, linhas)}
    assert itens["estavel"]["ratio"] == 1.0
    # (900/60 + 5) / (600/600 + 5) = 20 / 6
    assert itens["subindo"]["ratio"] == round(20 / 6, 2)
    # sem historico do termo, nao inventa crescimento
    assert itens["inedito"]["ratio"] is None
    assert [item["term"] for item in _crescimento(janelas, linhas)][0] == "subindo"


def test_crescimento_sem_janelas_nao_divide_por_zero():
    linhas = [{"term": "x", "kind": "term", "autores_1h": 5, "autores_base": 5}]
    itens = _crescimento({}, linhas)
    assert itens[0]["ratio"] is None


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
        if "uniqExactIf" in sql:
            return [{"janelas_1h": 60, "janelas_base": 600}]
        if "trending_terms" in sql:
            return [
                {
                    "term": "exemplo",
                    "kind": "term",
                    "autores_1h": 12,
                    "posts_1h": 20,
                    "autores_base": 40,
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


def test_ch_sink_vigia_detecta_travamento():
    # em 18/11/2026 o consumidor perdeu o Kafka e ficou 4 dias vivo sem gravar
    from producer.ch_sink import STALL_S, Watchdog

    agora = [1000.0]
    vigia = Watchdog(stall_s=600, clock=lambda: agora[0])
    agora[0] += 599
    assert not vigia.stalled()
    agora[0] += 2
    assert vigia.stalled()
    vigia.beat()
    assert not vigia.stalled()
    assert STALL_S <= 900, "o painel avisa com 15 min; o vigia age antes"


def test_ch_sink_nao_engole_erro_do_kafka_e_sai_quando_trava():
    fonte = (REPO_ROOT / "producer" / "src" / "producer" / "ch_sink.py").read_text(
        encoding="utf-8"
    )
    assert "if msg.error():\n                    continue" not in fonte
    assert 'logger.error("kafka_error' in fonte
    # sys.exit numa thread nao derruba o processo; o restart do Docker precisa
    # que o processo inteiro saia
    assert "os._exit(1)" in fonte
    assert "watchdog.beat()" in fonte
    compose = yaml.safe_load(COMPOSE.read_text(encoding="utf-8"))
    servico = compose["services"]["ch-sink"]
    assert servico["restart"] == "unless-stopped"
    assert float(servico["deploy"]["resources"]["limits"]["cpus"]) >= 0.25


def _totals(linha: dict) -> dict:
    import producer.ch_export as modulo

    consultas = []

    def fake_query(_url, sql):
        consultas.append(sql)
        return [linha]

    original = modulo.query
    modulo.query = fake_query
    try:
        return modulo.totals("http://clickhouse:8123"), consultas[0]
    finally:
        modulo.query = original


def test_totais_trazem_a_janela_mais_recente_da_tabela_mais_atrasada():
    totais, sql = _totals(
        {
            "ultima_volume": "2026-11-18 21:17:00",
            "ultima_trending": "2026-11-18 21:21:00",
        }
    )
    assert totais["latest_window"] == "2026-11-18T21:17:00Z"
    assert sql.count("window_start <= now() + INTERVAL 2 MINUTE") == 2


def test_totais_com_tabela_vazia_nao_inventam_data():
    # max() de tabela vazia no ClickHouse e 1970-01-01, nao NULL
    totais, _ = _totals(
        {
            "ultima_volume": "1970-01-01 00:00:00",
            "ultima_trending": "1970-01-01 00:00:00",
        }
    )
    assert totais["latest_window"] is None


def test_pagina_avisa_quando_o_banco_para_de_receber_janelas():
    html = PAGE.read_text(encoding="utf-8")
    assert "latest_window" in html
    assert "dados parados desde" in html


def test_ch_sink_descarta_janela_mais_velha_que_o_ttl():
    # createdAt de 2024 abria uma particao por dia e levou o ClickHouse ao
    # teto de memoria em 23/11/2026
    from producer.ch_sink import RETENTION, TRENDING_TOPIC, VOLUME_TOPIC, is_expired

    agora = datetime(2026, 11, 23, 1, 0, tzinfo=UTC)
    assert is_expired("2024-09-08 17:16:00", TRENDING_TOPIC, agora)
    assert is_expired("2026-11-16 00:59:00", TRENDING_TOPIC, agora)
    assert not is_expired("2026-11-16 01:01:00", TRENDING_TOPIC, agora)
    # volume guarda 30 dias
    assert not is_expired("2026-11-01 00:00:00", VOLUME_TOPIC, agora)
    assert is_expired("2026-10-20 00:00:00", VOLUME_TOPIC, agora)
    # a retencao do sink e a mesma do TTL das tabelas
    sql = INIT_SQL.read_text(encoding="utf-8")
    assert f"INTERVAL {RETENTION[TRENDING_TOPIC].days} DAY" in sql
    assert f"INTERVAL {RETENTION[VOLUME_TOPIC].days} DAY" in sql


def test_horas_de_historico_ignoram_janela_vencida():
    import producer.ch_export as modulo

    consultas = []

    def fake_query(_url, sql):
        consultas.append(sql)
        return [{"span": 3600}]

    original = modulo.query
    modulo.query = fake_query
    try:
        assert modulo._hours_of_data("http://clickhouse:8123") == 1.0
    finally:
        modulo.query = original
    assert "window_start >= now() - INTERVAL 7 DAY" in consultas[0]

    _, sql = _totals({})
    assert "window_start >= now() - INTERVAL 30 DAY) AS desde" in " ".join(sql.split())
