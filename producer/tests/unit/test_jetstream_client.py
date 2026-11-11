from producer.jetstream_client import build_subscribe_url


def test_build_url_with_filters():
    url = build_subscribe_url(
        "wss://host/xrpc/network.bsky.jetstream.subscribeEvents",
        ["app.bsky.feed.post"],
        ["commit"],
    )
    assert url == (
        "wss://host/xrpc/network.bsky.jetstream.subscribeEvents"
        "?collections=app.bsky.feed.post&kinds=commit"
    )


def test_build_url_with_cursor():
    url = build_subscribe_url(
        "wss://host/subscribe", ["app.bsky.feed.post"], ["commit"], cursor=987
    )
    assert "cursor=987" in url


def test_build_url_multiple_collections():
    url = build_subscribe_url("wss://h/s", ["a", "b"], ["commit"])
    assert "collections=a" in url and "collections=b" in url
