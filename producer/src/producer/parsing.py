from dataclasses import dataclass, field


class InvalidMessage(Exception):
    def __init__(self, stage: str, reason: str):
        super().__init__(f"{stage}: {reason}")
        self.stage = stage
        self.reason = reason


@dataclass(slots=True)
class HashtagEntity:
    tag: str
    start: int | None = None
    end: int | None = None


@dataclass(slots=True)
class PostEvent:
    uri: str
    did: str
    rkey: str
    event_time: str
    ingest_time: str
    seq: int
    text: str
    langs: list[str] = field(default_factory=list)
    self_labels: list[str] | None = None
    hashtag_entities: list[HashtagEntity] | None = None

    def to_dict(self) -> dict:
        return {
            "uri": self.uri,
            "did": self.did,
            "rkey": self.rkey,
            "event_time": self.event_time,
            "ingest_time": self.ingest_time,
            "seq": self.seq,
            "text": self.text,
            "langs": self.langs,
            "self_labels": self.self_labels,
            "hashtag_entities": (
                None
                if self.hashtag_entities is None
                else [
                    {"tag": h.tag, "start": h.start, "end": h.end}
                    for h in self.hashtag_entities
                ]
            ),
        }


def parse_frame(frame: bytes | str) -> dict:
    import json

    try:
        envelope = json.loads(frame)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise InvalidMessage("parse", f"json: {exc}") from exc
    if not isinstance(envelope, dict):
        raise InvalidMessage("parse", "envelope is not an object")
    payload = envelope.get("payload")
    if not isinstance(payload, dict):
        raise InvalidMessage("parse", "missing payload object")
    return payload


def is_commit_create(payload: dict) -> bool:
    return (
        payload.get("kind", "commit") == "commit"
        and payload.get("operation") == "create"
    )


def extract_hashtags(record: dict) -> list[HashtagEntity] | None:
    facets = record.get("facets")
    if not isinstance(facets, list) or not facets:
        return None
    entities: list[HashtagEntity] = []
    for facet in facets:
        if not isinstance(facet, dict):
            continue
        for feature in facet.get("features", []) or []:
            if (
                isinstance(feature, dict)
                and feature.get("$type") == "app.bsky.richtext.facet#tag"
                and isinstance(feature.get("tag"), str)
            ):
                index = facet.get("index") or {}
                entities.append(
                    HashtagEntity(
                        tag=feature["tag"],
                        start=index.get("byteStart"),
                        end=index.get("byteEnd"),
                    )
                )
    return entities or None


def extract_self_labels(record: dict) -> list[str] | None:
    """Rotulos que o proprio autor aplicou (labels.values[].val).

    Serve para tirar conteudo adulto dos agregados publicos sem depender de
    moderacao de terceiros. Ausente ou vazio vira None.
    """
    labels = record.get("labels")
    if isinstance(labels, list):
        values = labels
    elif isinstance(labels, dict):
        values = labels.get("values")
    else:
        return None
    if not isinstance(values, list):
        return None
    found: list[str] = []
    for item in values:
        if isinstance(item, dict) and isinstance(item.get("val"), str):
            found.append(item["val"])
        elif isinstance(item, str):
            found.append(item)
    return found or None


def build_post_event(payload: dict) -> PostEvent:
    try:
        did = payload["did"]
        rkey = payload["rkey"]
        seq = payload["seq"]
        collection = payload["collection"]
        record = payload["record"]
        ingest_time = payload["time"]
        text = record["text"]
        created_at = record["createdAt"]
        langs = record.get("langs") or []
    except (KeyError, TypeError) as exc:
        raise InvalidMessage("validate", f"missing field: {exc}") from exc

    if collection != "app.bsky.feed.post":
        raise InvalidMessage("validate", f"unexpected collection: {collection}")
    if not isinstance(seq, int):
        raise InvalidMessage("validate", "seq is not an integer")
    if not isinstance(text, str):
        raise InvalidMessage("validate", "text is not a string")
    if not isinstance(langs, list) or not all(isinstance(lang, str) for lang in langs):
        raise InvalidMessage("validate", "langs is not a list of strings")

    return PostEvent(
        uri=f"at://{did}/{collection}/{rkey}",
        did=did,
        rkey=rkey,
        event_time=created_at,
        ingest_time=ingest_time,
        seq=seq,
        text=text,
        langs=langs,
        self_labels=extract_self_labels(record),
        hashtag_entities=extract_hashtags(record),
    )
