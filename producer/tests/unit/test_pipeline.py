import pytest

from producer import Settings
from producer.main import Stats, process_frame


class FakePublisher:
    def __init__(self):
        self.posts = []
        self.dlq = []

    def publish_post(self, event, key):
        self.posts.append((event, key))

    def publish_dlq(self, raw, stage, reason):
        self.dlq.append((raw, stage, reason))


class FakeCursorStore:
    def __init__(self):
        self.saved = []

    def save(self, seq):
        self.saved.append(seq)


@pytest.fixture
def settings():
    return Settings(
        lang_primary_subtags=["pt"],
        cursor_persist_every=1,
        cursor_resume=False,
    )


async def test_pipeline_routes_frames(frames, settings):
    publisher = FakePublisher()
    store = FakeCursorStore()
    stats = Stats()
    cursor_state = {"seq": None}
    for frame in frames:
        await process_frame(frame, publisher, stats, store, cursor_state, settings)

    assert stats.received == len(frames)
    assert stats.published == 3
    assert stats.filtered_lang == 2
    assert stats.discarded_op == 2
    assert stats.dlq == 2
    assert len(publisher.posts) == 3
    assert len(publisher.dlq) == 2
    assert cursor_state["seq"] == 1008

    keys = [key for _, key in publisher.posts]
    assert keys == ["did:plc:1111", "did:plc:3333", "did:plc:7777"]
    assert store.saved[-1] == 1007


async def test_pipeline_dlq_reasons(frames, settings):
    publisher = FakePublisher()
    stats = Stats()
    cursor_state = {"seq": None}
    await process_frame(
        frames[5], publisher, stats, FakeCursorStore(), cursor_state, settings
    )
    await process_frame(
        frames[8], publisher, stats, FakeCursorStore(), cursor_state, settings
    )
    stages = {stage for _, stage, _ in publisher.dlq}
    assert stages == {"validate", "parse"}
