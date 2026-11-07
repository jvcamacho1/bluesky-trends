from producer.filters import is_portuguese, primary_subtag


def test_primary_subtag():
    assert primary_subtag("pt-BR") == "pt"
    assert primary_subtag("PT-PT") == "pt"
    assert primary_subtag("en") == "en"


def test_is_portuguese_pt_br():
    assert is_portuguese(["pt-BR"], ["pt"])


def test_is_portuguese_pt_pt():
    assert is_portuguese(["pt-PT"], ["pt"])


def test_is_portuguese_multilingual():
    assert is_portuguese(["en", "pt-BR"], ["pt"])


def test_is_not_portuguese():
    assert not is_portuguese(["en"], ["pt"])
    assert not is_portuguese(["es"], ["pt"])
    assert not is_portuguese([], ["pt"])


def test_is_not_portuguese_case_insensitive_subtags():
    assert is_portuguese(["pt-BR"], ["PT"])
