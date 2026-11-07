def primary_subtag(lang: str) -> str:
    return lang.split("-", 1)[0].strip().lower()


def is_portuguese(langs: list[str], primary_subtags: list[str]) -> bool:
    allowed = {s.lower() for s in primary_subtags}
    return any(primary_subtag(lang) in allowed for lang in langs)
