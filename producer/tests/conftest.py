import json
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
FIXTURES = Path(__file__).resolve().parent / "fixtures"
SCHEMA_PATH = ROOT / "schemas" / "post_event_v1.avsc"


@pytest.fixture
def schema() -> dict:
    with open(SCHEMA_PATH) as f:
        return json.load(f)


@pytest.fixture
def schema_path() -> str:
    return str(SCHEMA_PATH)


@pytest.fixture
def frames() -> list[bytes]:
    with open(FIXTURES / "jetstream_frames.jsonl", "rb") as f:
        return [line.strip() for line in f if line.strip()]
