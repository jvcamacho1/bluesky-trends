"""flink.json: janelas assentadas, minimo de autores, filtros, lacunas e privacidade."""

import json
from datetime import UTC, datetime, timedelta

from producer.flink_export import TRENDING_TOPIC, VOLUME_TOPIC, apply_message
from producer.flink_view import FlinkView, is_noise, parse_flink_time
from producer.public_terms import load_blocklist

NOW = datetime(2026, 11, 12, 15, 0, tzinfo=UTC)


def _ts(moment: datetime) -> str:
    return moment.strftime("%Y-%m-%d %H:%M:%S")


def _trend(view, end, term, authors, kind="hashtag", posts=None):
    start = end - timedelta(minutes=5)
    row = {
        "window_start": _ts(start),
        "window_end": _ts(end),
        "term": term,
        "kind": kind,
        "authors": authors,
        "posts": posts if posts is not None else authors,
    }
    assert view.add_trending({"window_start": row["window_start"]}, row)


def _volume(view, start, posts):
    row = {
        "window_start": _ts(start),
        "window_end": _ts(start + timedelta(minutes=1)),
        "posts": posts,
    }
    assert view.add_volume({"window_start": row["window_start"]}, row)


def _view(min_authors=10):
    return FlinkView(min_authors=min_authors, blocklist=load_blocklist())


SETTLED_END = NOW - timedelta(minutes=3)


def test_parse_flink_time_aceita_os_formatos_do_json_do_flink():
    expected = datetime(2026, 11, 12, 3, 5, tzinfo=UTC)
    assert parse_flink_time("2026-11-12 03:05:00") == expected
    assert parse_flink_time("2026-11-12 03:05:00.000") == expected
    assert parse_flink_time("2026-11-12T03:05:00Z") == expected
    assert parse_flink_time("lixo") is None
    assert parse_flink_time(None) is None


def test_nove_autores_nao_aparece_dez_aparece():
    view = _view()
    _trend(view, SETTLED_END, "nove", 9)
    _trend(view, SETTLED_END, "dez", 10)
    terms = [item["term"] for item in view.snapshot(NOW)["hashtags"]]
    assert terms == ["dez"]


def test_janela_ainda_aberta_nao_entra():
    view = _view()
    _trend(view, SETTLED_END, "assentada", 12)
    # fim depois de agora - 3 min: o Flink ainda pode regravar essa janela
    _trend(view, NOW - timedelta(minutes=1), "aberta", 50)
    data = view.snapshot(NOW)
    assert data["window"]["end"] == "2026-11-12T14:57:00Z"
    assert [i["term"] for i in data["hashtags"]] == ["assentada"]


def test_upsert_vale_o_ultimo_valor_e_tombstone_apaga():
    view = _view()
    _trend(view, SETTLED_END, "assunto", 4)
    _trend(view, SETTLED_END, "assunto", 15)
    assert view.snapshot(NOW)["hashtags"][0]["authors"] == 15
    key = {
        "window_start": _ts(SETTLED_END - timedelta(minutes=5)),
        "window_end": _ts(SETTLED_END),
        "term": "assunto",
        "kind": "hashtag",
    }
    assert view.add_trending(key, None)
    assert view.snapshot(NOW)["hashtags"] == []


def test_lista_de_bloqueio_palavra_vazia_e_ruido_ficam_fora():
    view = _view(min_authors=1)
    blocked = load_blocklist()[0]
    _trend(view, SETTLED_END, blocked, 40)
    _trend(view, SETTLED_END, "porque", 40, kind="term")
    _trend(view, SETTLED_END, "httpswwwyoutubecom", 40, kind="term")
    _trend(view, SETTLED_END, "fulanobskysocial", 40, kind="term")
    _trend(view, SETTLED_END, "eleicao", 40, kind="term")
    data = view.snapshot(NOW)
    assert data["hashtags"] == []
    assert [i["term"] for i in data["terms"]] == ["eleicao"]


def test_is_noise():
    assert is_noise("https")
    assert is_noise("wwwglobocom")
    assert is_noise("x" * 31)
    assert is_noise("grupolupagruposcom")
    assert is_noise("globocombr")
    assert not is_noise("copa")
    assert not is_noise("garrafao")


def test_ordem_por_autores_depois_posts():
    view = _view()
    _trend(view, SETTLED_END, "b", 20, posts=30)
    _trend(view, SETTLED_END, "a", 20, posts=90)
    _trend(view, SETTLED_END, "c", 25, posts=25)
    assert [i["term"] for i in view.snapshot(NOW)["hashtags"]] == ["c", "a", "b"]


def test_spark_tem_12_janelas_e_null_onde_nao_ha_janela():
    view = _view()
    for back in range(0, 12, 2):  # so as janelas pares existem
        _trend(view, SETTLED_END - timedelta(minutes=back), "copa", 10 + back)
    _trend(view, SETTLED_END - timedelta(minutes=1), "outro", 1)  # janela -1 existe
    spark = view.snapshot(NOW)["hashtags"][0]["spark"]
    assert len(spark) == 12
    assert spark[-1] == 10
    assert spark[-2] == 0  # janela existe, termo nao apareceu nela
    assert spark[-3] == 12
    assert spark[0] is None  # janela -11 nao existe no topico


def test_volume_tem_lacuna_null_e_ignora_minuto_aberto():
    view = _view()
    base = NOW - timedelta(minutes=10)
    _volume(view, base, 40)
    _volume(view, base + timedelta(minutes=2), 60)
    _volume(view, NOW - timedelta(minutes=1), 999)  # minuto ainda aberto
    data = view.snapshot(NOW)
    volume = data["volume"]
    assert len(volume) == 180
    assert volume[-1] == {"t": "2026-11-12T14:52:00Z", "posts": 60}
    assert volume[-2]["posts"] is None
    assert volume[-3]["posts"] == 40
    assert data["kpis"]["posts_last_minute"] == 60
    assert data["kpis"]["posts_last_hour"] == 100
    assert data["kpis"]["peak_minute"] == {"t": "2026-11-12T14:52:00Z", "posts": 60}


def test_sem_dados_nao_inventa_nada():
    data = _view().snapshot(NOW)
    assert data["window"] is None
    assert data["hashtags"] == [] and data["terms"] == [] and data["volume"] == []
    assert data["kpis"] == {
        "posts_last_minute": None,
        "posts_last_hour": None,
        "peak_minute": None,
        "event_delay_s": None,
    }


def test_prune_descarta_janelas_velhas():
    view = _view()
    _trend(view, NOW - timedelta(hours=2), "velho", 30)
    _volume(view, NOW - timedelta(hours=5), 10)
    view.snapshot(NOW)
    assert not view._trending
    assert not view._volume


def test_contrato_do_flink_json():
    view = _view()
    _trend(view, SETTLED_END, "copa", 12)
    _trend(view, SETTLED_END, "eleicao", 11, kind="term")
    _volume(view, NOW - timedelta(minutes=5), 50)
    data = view.snapshot(NOW)
    assert set(data) == {
        "generated_at",
        "min_authors",
        "window",
        "kpis",
        "hashtags",
        "terms",
        "volume",
    }
    assert set(data["window"]) == {"start", "end"}
    assert set(data["kpis"]) == {
        "posts_last_minute",
        "posts_last_hour",
        "peak_minute",
        "event_delay_s",
    }
    for item in data["hashtags"] + data["terms"]:
        assert set(item) == {"term", "authors", "posts", "spark"}
    assert data["generated_at"].endswith("Z")
    assert data["kpis"]["event_delay_s"] == 180


def test_privacidade_nenhuma_chave_ou_valor_proibido():
    view = _view(min_authors=1)
    _trend(view, SETTLED_END, "copa", 3)
    _volume(view, NOW - timedelta(minutes=5), 50)
    text = json.dumps(view.snapshot(NOW))

    def walk(node):
        if isinstance(node, dict):
            for key, value in node.items():
                assert key not in {"did", "uri", "rkey", "text"}
                walk(value)
        elif isinstance(node, list):
            for item in node:
                walk(item)
        elif isinstance(node, str):
            assert not node.startswith(("did:", "at://"))

    walk(json.loads(text))


def test_apply_message_roteia_por_topico_e_aceita_tombstone():
    view = _view()
    key = json.dumps({"window_start": "2026-11-12 14:56:00"}).encode()
    value = json.dumps(
        {
            "window_start": "2026-11-12 14:56:00",
            "window_end": "2026-11-12 14:57:00",
            "posts": 33,
        }
    ).encode()
    assert apply_message(view, VOLUME_TOPIC, key, value)
    assert view.snapshot(NOW)["kpis"]["posts_last_minute"] == 33
    assert apply_message(view, VOLUME_TOPIC, key, None)
    assert view.snapshot(NOW)["volume"] == []
    assert not apply_message(view, TRENDING_TOPIC, key, b"{nao e json")
    assert not apply_message(view, "outro.topico", key, value)
