from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Iterable, Iterator, Mapping, Protocol


@dataclass(frozen=True)
class SseEvent:
    event: str
    data: Any
    event_id: str | None = None
    retry: int | None = None


@dataclass(frozen=True)
class SseResponse:
    stream: Iterable[str]
    status: int = 200
    headers: dict[str, str] = field(default_factory=dict)
    content_type: str = "text/event-stream; charset=utf-8"

    def __post_init__(self) -> None:
        merged = {
            "Content-Type": self.content_type,
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        }
        merged.update(self.headers)
        object.__setattr__(self, "headers", merged)


class ActionDispatcher(Protocol):
    def execute(self, action: str, payload: Mapping[str, Any] | None = None, **kwargs) -> Any:
        ...


class SseStreamError(Exception):
    code = "internal_error"


class SsePermissionDenied(SseStreamError):
    code = "permission_denied"


class SseValidationError(SseStreamError):
    code = "validation_error"


class SseAdapter:
    """SSE transport projection over Muscles action execution."""

    allowed_events = {"progress", "log", "result", "error"}

    def __init__(self, dispatcher: ActionDispatcher, heartbeat_event: str | None = None):
        self.dispatcher = dispatcher
        self.heartbeat_event = heartbeat_event

    def stream_action(
        self,
        action: str,
        payload: Mapping[str, Any] | None = None,
        *,
        context: Mapping[str, Any] | None = None,
    ) -> SseResponse:
        context = dict(context or {})
        try:
            result = self.dispatcher.execute(action, payload, transport="sse", **context)
        except Exception as exc:
            mapped = self._map_error(exc)
            raise mapped from exc

        stream = self._iter_result(result)
        if self.heartbeat_event:
            stream = self._with_heartbeat(stream, self.heartbeat_event)
        return SseResponse(stream=stream)

    def _iter_result(self, result: Any) -> Iterator[str]:
        if isinstance(result, Iterable) and not isinstance(result, (str, bytes, bytearray, dict)):
            source = iter(result)
            try:
                for item in source:
                    yield self.format_event(self._coerce_event(item))
            finally:
                close = getattr(source, "close", None)
                if callable(close):
                    close()
            return
        yield self.format_event(SseEvent(event="result", data=result))

    def _with_heartbeat(self, stream: Iterable[str], event_name: str) -> Iterator[str]:
        for chunk in stream:
            yield chunk
            yield self.format_event(SseEvent(event=event_name, data={"ok": True}))

    def _coerce_event(self, item: Any) -> SseEvent:
        if isinstance(item, SseEvent):
            if item.event not in self.allowed_events and item.event != (self.heartbeat_event or ""):
                raise ValueError(f"Unknown SSE event type: {item.event}")
            return item
        if isinstance(item, Mapping):
            event = str(item.get("event", "progress"))
            data = item.get("data")
            event_id = item.get("id")
            retry = item.get("retry")
            coerced = SseEvent(event=event, data=data, event_id=event_id, retry=retry)
            if coerced.event not in self.allowed_events and coerced.event != (self.heartbeat_event or ""):
                raise ValueError(f"Unknown SSE event type: {coerced.event}")
            return coerced
        raise TypeError("SSE stream items must be SseEvent or mapping")

    @staticmethod
    def _map_error(exc: Exception) -> SseStreamError:
        name = exc.__class__.__name__.lower()
        if "permission" in name or "forbidden" in name:
            return SsePermissionDenied(str(exc))
        if "validation" in name:
            return SseValidationError(str(exc))
        return SseStreamError(str(exc))

    @staticmethod
    def format_event(event: SseEvent) -> str:
        lines = []
        if event.event_id is not None:
            lines.append(f"id: {event.event_id}")
        if event.retry is not None:
            lines.append(f"retry: {event.retry}")
        lines.append(f"event: {event.event}")
        lines.append(f"data: {json.dumps(event.data, ensure_ascii=False, default=str)}")
        lines.append("")
        return "\n".join(lines)
