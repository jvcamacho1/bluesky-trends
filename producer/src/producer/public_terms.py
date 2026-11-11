"""Tokenizacao e filtro de conteudo para os agregados publicos.

Regras da especificacao do painel publico:
- hashtags vem de hashtag_entities;
- termos do texto: minusculas, sem URLs, sem mencoes, sem pontuacao, sem
  palavras vazias do portugues, palavras com 3 letras ou mais, palavras
  isoladas e pares de palavras seguidas;
- um post sai de todos os agregados se o autor o marcou com rotulo proprio
  adulto ou se alguma hashtag/termo dele esta na lista de bloqueio.
"""

import re
import unicodedata
from pathlib import Path

BLOCKLIST_PATH = Path(__file__).with_name("blocklist_pt.txt")

# rotulos proprios que excluem o post dos agregados publicos
ADULT_SELF_LABELS = frozenset(
    {"porn", "sexual", "nudity", "graphic-media", "nsfw", "explicit"}
)

STOPWORDS_PATH = Path(__file__).with_name("stopwords_pt.txt")
STOPWORDS_PT = frozenset(
    line.strip()
    for line in STOPWORDS_PATH.read_text(encoding="utf-8").splitlines()
    if line.strip() and not line.startswith("#")
)

URL_RE = re.compile(r"https?://\S+|www\.\S+", re.IGNORECASE)
MENTION_RE = re.compile(r"@[\w.\-]+")
WORD_RE = re.compile(r"[a-z0-9]{3,}")


def _fold(text: str) -> str:
    """Minusculas e sem acento, para comparar termos de forma estavel."""
    lowered = text.lower()
    decomposed = unicodedata.normalize("NFKD", lowered)
    return "".join(ch for ch in decomposed if not unicodedata.combining(ch))


def load_blocklist(path: Path | str = BLOCKLIST_PATH) -> tuple[str, ...]:
    terms: list[str] = []
    for raw in Path(path).read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        terms.append(_fold(line))
    return tuple(terms)


def is_blocked(text: str, hashtags: list[str], blocklist: tuple[str, ...]) -> bool:
    """True se o post tem termo da lista de bloqueio na hashtag ou no texto."""
    if not blocklist:
        return False
    haystack = _fold(" ".join([text, *hashtags]))
    return any(term in haystack for term in blocklist)


def has_adult_self_label(self_labels: list[str] | None) -> bool:
    if not self_labels:
        return False
    return any(_fold(label) in ADULT_SELF_LABELS for label in self_labels)


def normalize_hashtag(tag: str) -> str:
    return _fold(tag).strip("#").strip()


def tokenize(text: str) -> list[str]:
    """Palavras isoladas, sem vazias nem curtas.

    Sem pares de palavras de proposito: os agregados publicos sao de termo
    unico, e bigrama de texto livre e o jeito mais facil de vazar uma frase.
    """
    cleaned = URL_RE.sub(" ", text)
    cleaned = MENTION_RE.sub(" ", cleaned)
    cleaned = _fold(cleaned)
    words = [w for w in WORD_RE.findall(cleaned) if w not in STOPWORDS_PT]
    return list(dict.fromkeys(words))
