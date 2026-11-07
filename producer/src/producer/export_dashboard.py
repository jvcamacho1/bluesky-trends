import argparse
import json
import sys
from collections import Counter
from datetime import datetime, timezone

from .consume_check import consume


def build_dashboard(messages: list[dict]) -> dict:
    posts = []
    hashtag_counter: Counter[str] = Counter()
    volume: Counter[str] = Counter()
    for message in sorted(messages, key=lambda m: m.get("seq", 0)):
        tags = [h["tag"] for h in (message.get("hashtag_entities") or [])]
        for tag in tags:
            hashtag_counter[tag.lower()] += 1
        event_time = message.get("event_time", "")
        minute = event_time[:16]
        if minute:
            volume[minute] += 1
        posts.append(
            {
                "text": message.get("text", ""),
                "did": message.get("did", ""),
                "event_time": event_time,
                "langs": message.get("langs", []),
                "hashtags": tags,
            }
        )
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "total_posts": len(posts),
        "top_hashtags": [
            {"tag": tag, "count": count}
            for tag, count in hashtag_counter.most_common(15)
        ],
        "volume_per_minute": [
            {"minute": minute, "count": count}
            for minute, count in sorted(volume.items())
        ],
        "latest_posts": posts[-50:],
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Exporta dados do topico Kafka para o dashboard estatico"
    )
    parser.add_argument("--bootstrap", default="localhost:29092")
    parser.add_argument("--registry", default="http://localhost:8081")
    parser.add_argument("--topic", default="bluesky.posts.pt.v1")
    parser.add_argument("--limit", type=int, default=500)
    parser.add_argument("--timeout", type=float, default=60.0)
    parser.add_argument("--output", default="docs/data.json")
    args = parser.parse_args()

    messages = consume(
        args.bootstrap, args.registry, args.topic, args.limit, args.timeout
    )
    if not messages:
        print("nenhuma mensagem consumida", file=sys.stderr)
        sys.exit(1)
    dashboard = build_dashboard(messages)
    with open(args.output, "w") as f:
        json.dump(dashboard, f, ensure_ascii=False, indent=2)
    print(f"{dashboard['total_posts']} posts exportados para {args.output}")


if __name__ == "__main__":
    main()
