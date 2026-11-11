import pytest

from producer.parsing import (
    InvalidMessage,
    build_post_event,
    extract_hashtags,
    is_commit_create,
    parse_frame,
)


def test_parse_frame_valid(frames):
    payload = parse_frame(frames[0])
    assert payload["did"] == "did:plc:1111"
    assert payload["seq"] == 1001


def test_parse_frame_malformed_json():
    with pytest.raises(InvalidMessage) as exc_info:
        parse_frame(b"{not json")
    assert exc_info.value.stage == "parse"


def test_parse_frame_missing_payload():
    with pytest.raises(InvalidMessage):
        parse_frame('{"$type": "message"}')


def test_is_commit_create(frames):
    assert is_commit_create(parse_frame(frames[0]))
    assert not is_commit_create(parse_frame(frames[3]))
    assert not is_commit_create(parse_frame(frames[4]))


def test_build_post_event(frames):
    event = build_post_event(parse_frame(frames[0]))
    assert event.uri == "at://did:plc:1111/app.bsky.feed.post/3labcdef1"
    assert event.event_time == "2026-08-13T06:47:44.859Z"
    assert event.ingest_time == "2026-08-13T06:47:43.959305Z"
    assert event.langs == ["pt-BR"]
    tags = [h.tag for h in event.hashtag_entities]
    assert tags == ["Brasil"]


def test_build_post_event_missing_text(frames):
    with pytest.raises(InvalidMessage) as exc_info:
        build_post_event(parse_frame(frames[5]))
    assert exc_info.value.stage == "validate"


def test_build_post_event_rejects_other_collection():
    payload = {
        "did": "did:plc:1",
        "rkey": "r",
        "seq": 1,
        "collection": "app.bsky.feed.like",
        "time": "2026-08-13T06:47:43.959305Z",
        "record": {"text": "x", "createdAt": "2026-08-13T06:47:44.859Z"},
    }
    with pytest.raises(InvalidMessage):
        build_post_event(payload)


def test_extract_hashtags_ignores_links(frames):
    record = parse_frame(frames[0])["record"]
    entities = extract_hashtags(record)
    assert entities is not None
    assert entities[0].tag == "Brasil"
    assert entities[0].start == 20
    assert entities[0].end == 27


def test_extract_hashtags_none_when_no_facets():
    assert extract_hashtags({"text": "sem facets"}) is None
