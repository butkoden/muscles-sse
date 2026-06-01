import pytest

from muscles_sse import SseAdapter, SseEvent


def test_sse_format_event():
    payload = SseAdapter.format_event(SseEvent(event="progress", data={"step": 1}, event_id="evt-1"))
    assert "id: evt-1" in payload
    assert "event: progress" in payload
    assert 'data: {"step": 1}' in payload
    assert payload.endswith("\n")


def test_sse_stream_order():
    adapter = SseAdapter()
    chunks = list(
        adapter.stream(
            [
                SseEvent(event="progress", data={"value": 10}),
                SseEvent(event="log", data={"message": "working"}),
                SseEvent(event="result", data={"ok": True}),
            ]
        )
    )
    assert "event: progress" in chunks[0]
    assert "event: log" in chunks[1]
    assert "event: result" in chunks[2]


def test_sse_rejects_unknown_event():
    adapter = SseAdapter()
    with pytest.raises(ValueError):
        list(adapter.stream([SseEvent(event="custom", data={})]))
