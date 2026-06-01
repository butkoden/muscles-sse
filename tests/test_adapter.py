import pytest

from muscles_sse import (
    SseAdapter,
    SseEvent,
    SsePermissionDenied,
    SseStreamError,
    SseValidationError,
)


class FakePermissionDenied(Exception):
    pass


class FakeValidationError(Exception):
    pass


class FakeDispatcher:
    def __init__(self, handler):
        self.handler = handler
        self.calls = []

    def execute(self, action, payload=None, **kwargs):
        self.calls.append((action, payload, kwargs))
        return self.handler(action, payload, kwargs)


def test_sse_format_event():
    payload = SseAdapter.format_event(SseEvent(event="progress", data={"step": 1}, event_id="evt-1", retry=1500))
    assert "id: evt-1" in payload
    assert "retry: 1500" in payload
    assert "event: progress" in payload
    assert 'data: {"step": 1}' in payload
    assert payload.endswith("\n")


def test_sse_stream_order_from_action_iterator():
    def handler(_action, _payload, _kwargs):
        return [
            SseEvent(event="progress", data={"value": 10}),
            {"event": "log", "data": {"message": "working"}},
            SseEvent(event="result", data={"ok": True}),
        ]

    dispatcher = FakeDispatcher(handler)
    adapter = SseAdapter(dispatcher)
    response = adapter.stream_action("bookings.export", {"id": 1})
    chunks = list(response.stream)

    assert "event: progress" in chunks[0]
    assert "event: log" in chunks[1]
    assert "event: result" in chunks[2]
    assert dispatcher.calls[0][2]["transport"] == "sse"


def test_sse_wraps_plain_result():
    dispatcher = FakeDispatcher(lambda *_: {"ok": True})
    adapter = SseAdapter(dispatcher)
    response = adapter.stream_action("bookings.export")
    chunks = list(response.stream)
    assert len(chunks) == 1
    assert "event: result" in chunks[0]
    assert 'data: {"ok": true}' in chunks[0]


def test_sse_rejects_unknown_event():
    dispatcher = FakeDispatcher(lambda *_: [SseEvent(event="custom", data={})])
    adapter = SseAdapter(dispatcher)
    with pytest.raises(ValueError):
        list(adapter.stream_action("bookings.export").stream)


def test_sse_permission_denied_blocks_stream():
    def handler(*_):
        raise FakePermissionDenied("denied")

    adapter = SseAdapter(FakeDispatcher(handler))
    with pytest.raises(SsePermissionDenied):
        adapter.stream_action("bookings.export")


def test_sse_validation_error_blocks_stream():
    def handler(*_):
        raise FakeValidationError("invalid")

    adapter = SseAdapter(FakeDispatcher(handler))
    with pytest.raises(SseValidationError):
        adapter.stream_action("bookings.export")


def test_sse_disconnect_closes_source():
    closed = {"ok": False}

    class Source:
        def __iter__(self):
            return self

        def __next__(self):
            raise StopIteration

        def close(self):
            closed["ok"] = True

    adapter = SseAdapter(FakeDispatcher(lambda *_: Source()))
    stream = adapter.stream_action("bookings.export").stream
    list(stream)
    assert closed["ok"] is True


def test_sse_heartbeat_policy():
    adapter = SseAdapter(
        FakeDispatcher(lambda *_: [SseEvent(event="progress", data={"step": 1})]),
        heartbeat_event="heartbeat",
    )
    chunks = list(adapter.stream_action("bookings.export").stream)
    assert "event: progress" in chunks[0]
    assert "event: heartbeat" in chunks[1]


def test_sse_response_headers_defaults():
    adapter = SseAdapter(FakeDispatcher(lambda *_: []))
    response = adapter.stream_action("bookings.export")
    assert response.headers["Content-Type"] == "text/event-stream; charset=utf-8"
    assert response.headers["Cache-Control"] == "no-cache"
    assert response.status == 200


def test_sse_application_scoped_state():
    d1 = FakeDispatcher(lambda *_: [])
    d2 = FakeDispatcher(lambda *_: [])
    a1 = SseAdapter(d1)
    a2 = SseAdapter(d2)
    list(a1.stream_action("a").stream)
    list(a2.stream_action("b").stream)
    assert d1.calls[0][0] == "a"
    assert d2.calls[0][0] == "b"


def test_sse_uses_same_action_business_handler_once():
    calls = {"count": 0}

    def use_case(_action, payload, _kwargs):
        calls["count"] += 1
        return {"echo": payload}

    adapter = SseAdapter(FakeDispatcher(use_case))
    chunks = list(adapter.stream_action("bookings.create", {"title": "Call"}).stream)
    assert calls["count"] == 1
    assert '"title": "Call"' in chunks[0]


def test_sse_unknown_error_maps_to_generic():
    def handler(*_):
        raise RuntimeError("boom")

    adapter = SseAdapter(FakeDispatcher(handler))
    with pytest.raises(SseStreamError):
        adapter.stream_action("bookings.export")
