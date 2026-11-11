from producer.cursor_store import CursorStore


def test_load_missing_file(tmp_path):
    store = CursorStore(str(tmp_path / "cursor.json"))
    assert store.load() is None


def test_save_and_load_roundtrip(tmp_path):
    store = CursorStore(str(tmp_path / "nested" / "cursor.json"))
    store.save(1234)
    assert store.load() == 1234


def test_save_is_atomic_overwrite(tmp_path):
    store = CursorStore(str(tmp_path / "cursor.json"))
    store.save(1)
    store.save(2)
    store.save(3)
    assert store.load() == 3
    assert list(tmp_path.glob("*.tmp")) == []
