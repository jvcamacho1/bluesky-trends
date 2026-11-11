"""Tokenizacao e filtro de conteudo dos agregados publicos."""

from producer.public_terms import (
    has_adult_self_label,
    is_blocked,
    load_blocklist,
    normalize_hashtag,
    tokenize,
)


def test_tokenize_remove_urls_mentions_e_pontuacao():
    tokens = tokenize(
        "Olha isso https://exemplo.com/a?b=1 @fulano, MUITO bom! #tag não entra"
    )
    assert "exemplo" not in tokens
    assert "https" not in tokens
    assert "fulano" not in tokens
    assert "olha" in tokens
    assert "tag" in tokens


def test_tokenize_remove_palavras_vazias_e_curtas():
    tokens = tokenize("a de o que eu ia ao cinema com ela")
    assert "que" not in tokens
    assert "com" not in tokens
    assert "ela" not in tokens
    assert "cinema" in tokens


def test_tokenize_nao_gera_pares_de_palavras():
    tokens = tokenize("seleção brasileira joga hoje")
    assert "selecao" in tokens
    assert "brasileira" in tokens
    assert all(" " not in token for token in tokens)


def test_tokenize_normaliza_acentos_e_repeticao():
    tokens = tokenize("Eleições eleicoes ELEICOES")
    assert tokens.count("eleicoes") == 1


def test_normalize_hashtag():
    assert normalize_hashtag("#Brasil") == "brasil"
    assert normalize_hashtag("Eleições") == "eleicoes"


def test_blocklist_carrega_e_ignora_comentarios():
    terms = load_blocklist()
    assert "pornografia" in terms
    assert all(not term.startswith("#") for term in terms)


def test_is_blocked_por_hashtag_e_por_texto():
    blocklist = load_blocklist()
    assert is_blocked("qualquer coisa", ["pornografia"], blocklist)
    assert is_blocked("conteudo explicito aqui", [], blocklist)
    assert not is_blocked("jogo do brasil hoje", ["brasil"], blocklist)


def test_rotulo_proprio_adulto():
    assert has_adult_self_label(["porn"])
    assert has_adult_self_label(["sexual", "foo"])
    assert not has_adult_self_label(["news"])
    assert not has_adult_self_label(None)
