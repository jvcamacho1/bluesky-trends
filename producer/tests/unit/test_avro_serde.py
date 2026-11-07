import io
import json
import struct
from unittest.mock import patch

import avro.schema
import pytest
from avro.io import BinaryDecoder, DatumReader

from producer.avro_serde import AvroPostSerializer


class FakeResponse:
    def __init__(self, status_code, payload):
        self.status_code = status_code
        self._payload = payload
        self.text = str(payload)

    def json(self):
        return self._payload


@pytest.fixture
def serializer(schema_path, schema):
    with (
        patch("producer.avro_serde.requests.put", return_value=FakeResponse(200, {})),
        patch(
            "producer.avro_serde.requests.post",
            return_value=FakeResponse(200, {"id": 42}),
        ),
    ):
        s = AvroPostSerializer(schema_path, "http://registry:8081", "subject-value")
        s.register()
        yield s


def test_register_sets_schema_id(serializer):
    assert serializer.schema_id == 42


def test_serialize_wire_format(serializer, schema):
    event = {
        "uri": "at://did:plc:1/app.bsky.feed.post/r1",
        "did": "did:plc:1",
        "rkey": "r1",
        "event_time": "2026-08-13T06:47:44.859Z",
        "ingest_time": "2026-08-13T06:47:43.959305Z",
        "seq": 1001,
        "text": "olá #Brasil",
        "langs": ["pt-BR"],
        "hashtag_entities": [{"tag": "Brasil", "start": 5, "end": 12}],
    }
    wire = serializer.serialize(event)
    magic, schema_id = struct.unpack(">bI", wire[:5])
    assert magic == 0
    assert schema_id == 42

    parsed = avro.schema.parse(json.dumps(schema))
    decoded = DatumReader(parsed).read(BinaryDecoder(io.BytesIO(wire[5:])))
    assert decoded == event


def test_serialize_null_hashtags(serializer):
    event = {
        "uri": "at://did:plc:2/app.bsky.feed.post/r2",
        "did": "did:plc:2",
        "rkey": "r2",
        "event_time": "2026-08-13T06:47:44.859Z",
        "ingest_time": "2026-08-13T06:47:43.959305Z",
        "seq": 1002,
        "text": "sem hashtags",
        "langs": ["pt"],
        "hashtag_entities": None,
    }
    wire = serializer.serialize(event)
    assert len(wire) > 5
