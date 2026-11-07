from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    jetstream_url: str = (
        "wss://jetstream.us-east.bsky.network"
        "/xrpc/network.bsky.jetstream.subscribeEvents"
    )
    collections: list[str] = ["app.bsky.feed.post"]
    kinds: list[str] = ["commit"]
    input_mode: str = "ws"
    input_file: str = ""
    lang_primary_subtags: list[str] = ["pt"]
    cursor_path: str = "/data/cursor.json"
    cursor_resume: bool = True
    cursor_persist_every: int = 100
    reconnect_max_backoff_s: float = 60.0
    stats_every_s: float = 30.0

    kafka_bootstrap_servers: str = "127.0.0.1:29092"
    kafka_topic: str = "bluesky.posts.pt.v1"
    kafka_dlq_topic: str = "bluesky.posts.pt.dlq.v1"
    kafka_client_id: str = "bluesky-jetstream-producer"
    kafka_linger_ms: int = 5
    kafka_compression: str = "lz4"
    kafka_message_max_retries: int = 5

    schema_registry_url: str = "http://localhost:8081"
    schema_path: str = "/app/schemas/post_event_v1.avsc"
    schema_subject: str = "bluesky.posts.pt.v1-value"
    schema_compatibility: str = "BACKWARD"


@lru_cache
def get_settings() -> Settings:
    return Settings()
