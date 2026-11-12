"""Contrato do job Flink da fase 2: SQL, conectores e limites.

Nao sobe cluster: le os arquivos e confere as regras que, se quebrarem, so
aparecem em producao (formato do topico, watermark, janelas, orcamento).
"""

import re
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[3]
FLINK_DIR = REPO_ROOT / "flink"
SQL_DIR = FLINK_DIR / "sql"
COMPOSE = REPO_ROOT / "docker-compose.yml"
COMPOSE_FLINK = REPO_ROOT / "docker-compose.flink.yml"


def _sql(name: str) -> str:
    return (SQL_DIR / name).read_text(encoding="utf-8")


def test_sources_leem_avro_do_schema_registry():
    sql = _sql("01_sources.sql")
    assert "'format' = 'avro-confluent'" in sql
    assert "'avro-confluent.url' = 'http://schema-registry:8081'" in sql
    assert "'topic' = 'bluesky.posts.pt.v1'" in sql


def test_watermark_usa_tempo_de_evento():
    sql = _sql("01_sources.sql")
    assert "WATERMARK FOR ts" in sql
    assert "event_time" in sql
    # ingest_time nao pode virar tempo de janela: ele mede latencia
    assert (
        "WATERMARK FOR ts AS" in sql
        and "ingest_time" not in sql.split("WATERMARK")[1].split(")")[0]
    )


def test_janelas_hop_e_tumble():
    hashtags = _sql("03_jobs.sql")
    termos = _sql("05_terms.sql")
    volume = _sql("04_volume.sql")
    for sql in (hashtags, termos):
        assert (
            "HOP(TABLE posts, DESCRIPTOR(ts), INTERVAL '1' MINUTE, INTERVAL '5' MINUTE)"
            in sql
        )
    assert "TUMBLE(TABLE posts, DESCRIPTOR(ts), INTERVAL '1' MINUTE)" in volume


def test_trending_conta_autores_distintos():
    for name in ("03_jobs.sql", "05_terms.sql"):
        assert "COUNT(DISTINCT p.did)" in _sql(name) or "COUNT(DISTINCT did)" in _sql(
            name
        )


def test_cada_job_filtra_tempo_de_evento_invalido():
    # sem este filtro o Flink derruba o job com "RowTime field should not be null"
    for name in ("03_jobs.sql", "04_volume.sql", "05_terms.sql"):
        sql = _sql(name)
        assert "ts IS NOT NULL" in sql, name
        assert "CURRENT_TIMESTAMP + INTERVAL '1' DAY" in sql, name


def test_sinks_sao_upsert_kafka():
    sql = _sql("02_sinks.sql")
    assert sql.count("'connector' = 'upsert-kafka'") == 2
    assert "bluesky.trending.v1" in sql
    assert "bluesky.volume.v1" in sql


def test_submit_envia_os_tres_jobs():
    script = (FLINK_DIR / "submit.sh").read_text(encoding="utf-8")
    for name in ("03_jobs.sql", "04_volume.sql", "05_terms.sql"):
        assert name in script
    # cada job precisa das fontes e sinks na mesma sessao
    assert "BASE=" in script


def test_flink_dentro_do_orcamento_e_sem_porta_publica():
    compose = yaml.safe_load(COMPOSE.read_text(encoding="utf-8"))
    flink = yaml.safe_load(COMPOSE_FLINK.read_text(encoding="utf-8"))
    servicos = {**compose["services"], **flink["services"]}

    total_mem = 0
    total_cpu = 0.0
    for nome, servico in servicos.items():
        limites = ((servico.get("deploy") or {}).get("resources") or {}).get(
            "limits"
        ) or {}
        memoria = str(limites.get("memory", "0"))
        total_mem += (
            int(memoria[:-1]) / 1024 if memoria.endswith("m") else int(memoria[:-1])
        )
        total_cpu += float(limites.get("cpus", 0))
        for porta in servico.get("ports", []) or []:
            assert str(porta).startswith("127.0.0.1:"), f"{nome}: {porta}"
    assert total_mem <= 7, f"memoria total {total_mem:.2f} GB"
    assert total_cpu <= 3.0, f"cpus totais {total_cpu:.2f}"


def test_flink_tem_heap_explicito():
    compose = yaml.safe_load(
        (REPO_ROOT / "docker-compose.flink.yml").read_text(encoding="utf-8")
    )
    for nome in ("flink-jobmanager", "flink-taskmanager"):
        props = compose["services"][nome]["environment"]["FLINK_PROPERTIES"]
        assert "process.size" in props, nome
        assert re.search(
            r"(jobmanager|taskmanager)\.memory\.process\.size: \d+m", props
        ), nome
