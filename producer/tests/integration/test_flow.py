import os
import shutil
import subprocess
import time

import pytest

pytestmark = pytest.mark.integration

COMPOSE = os.environ.get("COMPOSE_CMD", "docker compose")
REPO_ROOT = os.environ.get(
    "REPO_ROOT",
    os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..")),
)
FIXTURE = "jetstream_frames.jsonl"
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:29092")
REGISTRY = os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081")


def compose(*args, check=True):
    result = subprocess.run(
        COMPOSE.split() + list(args),
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )
    if check and result.returncode != 0:
        raise AssertionError(
            f"compose {args} failed:\n{result.stdout}\n{result.stderr}"
        )
    return result


@pytest.fixture(scope="module")
def stack():
    if shutil.which("docker") is None:
        pytest.skip("docker indisponivel")
    compose("up", "-d", "--build", "kafka", "schema-registry")
    for topic in ("bluesky.posts.pt.v1", "bluesky.posts.pt.dlq.v1"):
        compose(
            "exec",
            "-T",
            "kafka",
            "kafka-topics",
            "--bootstrap-server",
            "kafka:9092",
            "--delete",
            "--topic",
            topic,
            check=False,
        )
    compose("up", "kafka-init")
    import urllib.request

    deadline = time.time() + 90
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(REGISTRY + "/subjects", timeout=2):
                break
        except OSError:
            time.sleep(2)
    else:
        raise AssertionError("schema registry nao respondeu a tempo")
    yield
    compose("down", check=False)


@pytest.fixture(scope="module")
def produced(stack):
    compose(
        "run",
        "--rm",
        "--no-deps",
        "-e",
        "INPUT_MODE=file",
        "-e",
        f"INPUT_FILE=/fixtures/{FIXTURE}",
        "-e",
        "CURSOR_PATH=/tmp/cursor.json",
        "-e",
        "CURSOR_RESUME=false",
        "-e",
        "KAFKA_BOOTSTRAP_SERVERS=kafka:9092",
        "-e",
        "SCHEMA_REGISTRY_URL=http://schema-registry:8081",
        "producer",
    )
    return True


def test_posts_topic_receives_portuguese_posts(produced):
    from producer.consume_check import consume

    messages = consume(BOOTSTRAP, REGISTRY, "bluesky.posts.pt.v1", 3, timeout_s=30)
    assert len(messages) == 3
    for message in messages:
        assert message["did"].startswith("did:plc:")
        assert message["text"]
        assert any(lang.startswith("pt") for lang in message["langs"])
    texts = [m["text"] for m in messages]
    assert "Que jogo incrível! #Brasil #Futebol" in texts


def test_dlq_receives_invalid_messages(produced):
    from producer.consume_check import consume

    messages = consume(
        BOOTSTRAP,
        REGISTRY,
        "bluesky.posts.pt.dlq.v1",
        2,
        timeout_s=30,
        raw=True,
    )
    assert len(messages) == 2
    reasons = {m["_headers"]["error.reason"] for m in messages}
    assert any("text" in reason for reason in reasons)
    assert any("json" in reason for reason in reasons)
