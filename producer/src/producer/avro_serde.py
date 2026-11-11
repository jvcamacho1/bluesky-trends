import io
import json
import struct

import avro.schema
import requests
from avro.io import BinaryEncoder, DatumWriter


class SchemaRegistryError(RuntimeError):
    pass


class AvroPostSerializer:
    def __init__(
        self,
        schema_path: str,
        registry_url: str,
        subject: str,
        compatibility: str = "BACKWARD",
    ):
        with open(schema_path) as f:
            self.schema = json.load(f)
        self.registry_url = registry_url.rstrip("/")
        self.subject = subject
        self.compatibility = compatibility
        self._schema_id: int | None = None
        self.parsed_schema = avro.schema.parse(json.dumps(self.schema))

    def register(self) -> int:
        config_url = f"{self.registry_url}/config/{self.subject}"
        requests.put(config_url, json={"compatibility": self.compatibility}, timeout=10)
        response = requests.post(
            f"{self.registry_url}/subjects/{self.subject}/versions",
            json={"schemaType": "AVRO", "schema": json.dumps(self.schema)},
            timeout=10,
        )
        if response.status_code not in (200, 409):
            raise SchemaRegistryError(
                f"schema registration failed: {response.status_code} {response.text}"
            )
        self._schema_id = response.json()["id"]
        return self._schema_id

    @property
    def schema_id(self) -> int:
        if self._schema_id is None:
            self.register()
        assert self._schema_id is not None
        return self._schema_id

    def serialize(self, event: dict) -> bytes:
        buffer = io.BytesIO()
        writer = DatumWriter(self.parsed_schema)
        writer.write(event, BinaryEncoder(buffer))
        return struct.pack(">bI", 0, self.schema_id) + buffer.getvalue()
